// Package tenantpool provides a tenant-aware Postgres connection pool
// registry for Go HTTP services.
//
// A single Registry serves both single-database and multi-tenant
// deployments through the same API. The module code that uses the
// Registry never sees whether there is one pool or one-per-tenant, nor
// which pooler (Supavisor, PgBouncer, direct PG) sits behind the DSN —
// that detail lives entirely in the Config passed to New.
//
// # Single-database mode
//
//	reg, err := tenantpool.New(tenantpool.Config{
//	    DatabaseURL: "postgres://user:pw@host:5432/db",
//	})
//
// # Multi-tenant mode
//
//	reg, err := tenantpool.New(tenantpool.Config{
//	    DSNBuilder: tenantpool.SupavisorDSN(tenantpool.SupavisorOpts{
//	        Host: "supavisor:5432", Schema: "auth", Password: "…",
//	    }),
//	    Resolver: tenantpool.HeaderResolver("X-Tenant-Ref"),
//	})
//
// Either way, handlers read the pool through pgctx helpers:
//
//	pool := tenantpool.PoolFromCtx(r.Context())
package tenantpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/singleflight"
)

// Config describes how a Registry resolves pools.
//
// Exactly one of DatabaseURL or DSNBuilder must be set. DatabaseURL puts
// the Registry in single-database mode (every Get returns the same
// pool). DSNBuilder puts it in multi-tenant mode, in which case Resolver
// is also required so HTTP middleware can identify the tenant for each
// request. Background workers that call Get directly do not need a
// Resolver.
type Config struct {
	// DatabaseURL puts the Registry in single-database mode. Ignored when
	// DSNBuilder is set.
	DatabaseURL string

	// DSNBuilder returns the connection string for a given tenant ID.
	// Required in multi-tenant mode. Typical implementations come from
	// the SupavisorDSN / PgBouncerDSN / DirectDSN helpers in this
	// package, or a custom closure built by the caller.
	DSNBuilder func(tenantID string) (string, error)

	// Resolver extracts the tenant ID from an HTTP request for use by
	// Middleware. Optional when the Registry is only consumed by
	// background workers that call Get with an explicit tenant ID.
	Resolver Resolver

	// MaxPoolsCached caps the number of live tenant pools. When exceeded,
	// the least-recently-used pool is closed in the background.
	// Default 500. Ignored in single-database mode.
	MaxPoolsCached int

	// IdleTimeout closes pools that have not been accessed for this
	// duration. Default 15 minutes. Ignored in single-database mode.
	IdleTimeout time.Duration

	// MaxConnsPerPool caps pgxpool.MaxConns on each tenant pool. Default
	// 10.
	MaxConnsPerPool int32

	// MinConnsPerPool caps pgxpool.MinConns on each tenant pool. Default
	// 0 — no idle connections held for unused tenants.
	MinConnsPerPool int32

	// ConfigurePool lets callers tweak a pgxpool.Config after parsing
	// and before NewWithConfig. Use it for statement-cache settings,
	// lifetime caps, tracers, or extra TLS configuration. Runs once per
	// pool creation. Optional.
	ConfigurePool func(*pgxpool.Config) error
}

// Registry is the tenant pool cache. Safe for concurrent use.
//
// Callers must call Close at service shutdown to drain pools cleanly.
type Registry struct {
	cfg Config

	// singleFlight de-duplicates concurrent Get calls for the same
	// tenant so only one pool is ever created per key.
	sf singleflight.Group

	mu     sync.Mutex
	cache  map[string]*poolEntry
	single *pgxpool.Pool // non-nil in single-database mode

	// Counters snapshot via Stats.
	created uint64
	evicted uint64
	hits    uint64
	misses  uint64
	errors  uint64

	metrics metricsState
}

type poolEntry struct {
	pool       *pgxpool.Pool
	lastAccess time.Time
}

// New constructs a Registry from Config. Validation errors return
// ErrInvalidConfig. In multi-tenant mode nothing is dialled until Get is
// called. In single-database mode the single pool is opened eagerly so
// configuration errors surface at startup.
func New(cfg Config) (*Registry, error) {
	if cfg.DatabaseURL == "" && cfg.DSNBuilder == nil {
		return nil, fmt.Errorf("%w: either DatabaseURL or DSNBuilder must be set", ErrInvalidConfig)
	}
	if cfg.DatabaseURL != "" && cfg.DSNBuilder != nil {
		return nil, fmt.Errorf("%w: DatabaseURL and DSNBuilder are mutually exclusive", ErrInvalidConfig)
	}
	applyDefaults(&cfg)

	r := &Registry{
		cfg:   cfg,
		cache: make(map[string]*poolEntry),
	}

	if cfg.DatabaseURL != "" {
		pool, err := r.buildPool(context.Background(), cfg.DatabaseURL)
		if err != nil {
			return nil, fmt.Errorf("open single pool: %w", err)
		}
		r.single = pool
	}
	return r, nil
}

func applyDefaults(c *Config) {
	if c.MaxPoolsCached == 0 {
		c.MaxPoolsCached = 500
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 15 * time.Minute
	}
	if c.MaxConnsPerPool == 0 {
		c.MaxConnsPerPool = 10
	}
}

// Get returns a pool for tenantID. In single-database mode the tenantID
// is ignored and the shared pool is returned.
//
// Concurrent Get calls for the same tenantID are de-duplicated; only
// one pool is ever opened per key. Errors are classified through the
// sentinel set defined in errors.go.
func (r *Registry) Get(ctx context.Context, tenantID string) (*pgxpool.Pool, error) {
	if r.single != nil {
		return r.single, nil
	}
	if tenantID == "" {
		return nil, fmt.Errorf("%w: empty tenantID", ErrInvalidConfig)
	}

	now := time.Now()
	r.mu.Lock()
	if entry, ok := r.cache[tenantID]; ok {
		entry.lastAccess = now
		r.hits++
		r.mu.Unlock()
		return entry.pool, nil
	}
	r.misses++
	r.mu.Unlock()

	// Use singleflight to prevent duplicate pool creation under concurrency.
	v, err, _ := r.sf.Do(tenantID, func() (any, error) {
		// Re-check inside singleflight in case another goroutine just
		// populated the cache for us.
		r.mu.Lock()
		if entry, ok := r.cache[tenantID]; ok {
			entry.lastAccess = time.Now()
			r.mu.Unlock()
			return entry.pool, nil
		}
		r.mu.Unlock()

		dsn, err := r.cfg.DSNBuilder(tenantID)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrTenantNotFound, err)
		}
		pool, err := r.buildPool(ctx, dsn)
		if err != nil {
			return nil, err
		}

		r.mu.Lock()
		r.cache[tenantID] = &poolEntry{pool: pool, lastAccess: time.Now()}
		r.created++
		evicted := 0
		if len(r.cache) > r.cfg.MaxPoolsCached {
			if r.evictOldestLocked() {
				evicted = 1
			}
		}
		r.mu.Unlock()
		r.metrics.incCreated()
		r.metrics.incEvicted(evicted)
		return pool, nil
	})
	if err != nil {
		r.mu.Lock()
		r.errors++
		r.mu.Unlock()
		r.metrics.incError(classifyError(err))
		return nil, err
	}
	return v.(*pgxpool.Pool), nil
}

func (r *Registry) buildPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: parse dsn: %w", ErrInvalidConfig, err)
	}
	cfg.MaxConns = r.cfg.MaxConnsPerPool
	cfg.MinConns = r.cfg.MinConnsPerPool

	if r.cfg.ConfigurePool != nil {
		if err := r.cfg.ConfigurePool(cfg); err != nil {
			return nil, fmt.Errorf("%w: configure pool: %w", ErrInvalidConfig, err)
		}
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUpstreamUnreachable, err)
	}
	return pool, nil
}

// evictOldestLocked drops the least-recently-used entry. Called under
// r.mu. Pool close runs in a background goroutine so the hot path does
// not block on in-flight queries draining. Returns true if an entry
// was evicted.
func (r *Registry) evictOldestLocked() bool {
	var oldestKey string
	var oldestTime time.Time
	first := true
	for k, e := range r.cache {
		if first || e.lastAccess.Before(oldestTime) {
			oldestKey = k
			oldestTime = e.lastAccess
			first = false
		}
	}
	if oldestKey == "" {
		return false
	}
	entry := r.cache[oldestKey]
	delete(r.cache, oldestKey)
	r.evicted++
	go entry.pool.Close()
	return true
}

// EvictIdle closes every pool that has not been accessed within
// IdleTimeout. Call periodically from a janitor goroutine (once a
// minute is plenty). Returns the number of pools evicted.
//
// No-op in single-database mode.
func (r *Registry) EvictIdle() int {
	if r.single != nil {
		return 0
	}
	cutoff := time.Now().Add(-r.cfg.IdleTimeout)
	r.mu.Lock()
	var toClose []*pgxpool.Pool
	for k, e := range r.cache {
		if e.lastAccess.Before(cutoff) {
			toClose = append(toClose, e.pool)
			delete(r.cache, k)
			r.evicted++
		}
	}
	r.mu.Unlock()
	for _, p := range toClose {
		go p.Close()
	}
	r.metrics.incEvicted(len(toClose))
	return len(toClose)
}

// Invalidate drops the pool for tenantID. Use this when tenant metadata
// changes (password rotated, tenant paused). No-op in single mode.
func (r *Registry) Invalidate(tenantID string) {
	if r.single != nil {
		return
	}
	r.mu.Lock()
	entry, ok := r.cache[tenantID]
	if ok {
		delete(r.cache, tenantID)
		r.evicted++
	}
	r.mu.Unlock()
	if ok {
		go entry.pool.Close()
		r.metrics.incEvicted(1)
	}
}

// Close drains all pools and clears the cache. Call once at service
// shutdown. Safe to call multiple times.
func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.single != nil {
		r.single.Close()
		r.single = nil
	}
	for _, e := range r.cache {
		e.pool.Close()
	}
	r.cache = make(map[string]*poolEntry)
}

// Stats is a snapshot of Registry counters. Intended for logging;
// metrics wire into Prometheus via the metrics.go hooks.
type Stats struct {
	ActivePools int
	Created     uint64
	Evicted     uint64
	Hits        uint64
	Misses      uint64
	Errors      uint64
	SingleMode  bool
}

// Stats returns the current counter snapshot.
func (r *Registry) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Stats{
		ActivePools: len(r.cache),
		Created:     r.created,
		Evicted:     r.evicted,
		Hits:        r.hits,
		Misses:      r.misses,
		Errors:      r.errors,
		SingleMode:  r.single != nil,
	}
}

// classifyError maps an error to the metrics label used by the
// pool_errors_total counter. Unknown errors fold into "other".
func classifyError(err error) string {
	switch {
	case errors.Is(err, ErrTenantNotFound):
		return "tenant_not_found"
	case errors.Is(err, ErrUpstreamUnreachable):
		return "upstream_unreachable"
	case errors.Is(err, ErrPoolExhausted):
		return "pool_exhausted"
	case errors.Is(err, ErrInvalidConfig):
		return "invalid_config"
	default:
		return "other"
	}
}
