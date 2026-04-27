package tenantpool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// PasswordResolver returns the database role password for a given
// tenant. Implementations decide where the password lives — Azure Key
// Vault, an in-memory map for tests, a file mount per tenant, etc. —
// and how aggressively to cache.
//
// Resolvers run on the hot path of pgxpool.New whenever the registry
// needs to open a tenant pool, so they should be cheap on cache hits
// and bounded on cache misses (one upstream call, idempotent under
// concurrency). Errors propagate up as ErrTenantNotFound.
type PasswordResolver interface {
	Resolve(ctx context.Context, tenantID string) (string, error)
}

// PasswordResolverFunc adapts a plain function into a PasswordResolver.
// Use it for one-off test stubs: `tenantpool.PasswordResolverFunc(func(_, t)
// (string, error) { return "secret", nil })`.
type PasswordResolverFunc func(ctx context.Context, tenantID string) (string, error)

// Resolve implements PasswordResolver.
func (f PasswordResolverFunc) Resolve(ctx context.Context, tenantID string) (string, error) {
	return f(ctx, tenantID)
}

// StaticPasswordResolver returns the same password for every tenant.
// Useful in dev when one shared role serves every tenant DB, and as a
// migration step from the legacy "global env var" model that predates
// per-tenant secrets.
//
// Production callers should prefer a real KV-backed resolver — sharing
// a single password across tenants kills the blast-radius story when
// any one tenant's pod gets compromised.
func StaticPasswordResolver(pw string) PasswordResolver {
	return PasswordResolverFunc(func(_ context.Context, _ string) (string, error) {
		if pw == "" {
			return "", errors.New("tenantpool: static password is empty")
		}
		return pw, nil
	})
}

// CachingPasswordResolver wraps another resolver with an in-memory
// cache that expires entries after ttl. Concurrent Resolve calls for
// the same tenant share a single underlying fetch (singleflight-like
// semantics via the registry's existing singleflight group is not
// reused here so the resolver stays decoupled — one extra fetch under
// contention is cheap and bounded).
//
// CachingPasswordResolver is goroutine-safe.
type CachingPasswordResolver struct {
	inner PasswordResolver
	ttl   time.Duration
	mu    sync.RWMutex
	cache map[string]cachedPassword
}

type cachedPassword struct {
	value string
	exp   time.Time
}

// NewCachingPasswordResolver wraps inner with ttl-bounded caching.
// ttl<=0 disables caching (every Resolve hits the inner resolver).
func NewCachingPasswordResolver(inner PasswordResolver, ttl time.Duration) *CachingPasswordResolver {
	if inner == nil {
		panic("tenantpool: NewCachingPasswordResolver: inner resolver must not be nil")
	}
	return &CachingPasswordResolver{
		inner: inner,
		ttl:   ttl,
		cache: make(map[string]cachedPassword),
	}
}

// Resolve returns the cached password for tenantID if still valid,
// otherwise delegates to the inner resolver and caches the result.
func (r *CachingPasswordResolver) Resolve(ctx context.Context, tenantID string) (string, error) {
	if r.ttl > 0 {
		r.mu.RLock()
		if e, ok := r.cache[tenantID]; ok && time.Now().Before(e.exp) {
			r.mu.RUnlock()
			return e.value, nil
		}
		r.mu.RUnlock()
	}

	pw, err := r.inner.Resolve(ctx, tenantID)
	if err != nil {
		return "", err
	}
	if r.ttl > 0 {
		r.mu.Lock()
		r.cache[tenantID] = cachedPassword{value: pw, exp: time.Now().Add(r.ttl)}
		r.mu.Unlock()
	}
	return pw, nil
}

// Invalidate forces the next Resolve for tenantID to bypass the cache.
// Pair with Registry.Invalidate when a tenant's password rotates.
func (r *CachingPasswordResolver) Invalidate(tenantID string) {
	r.mu.Lock()
	delete(r.cache, tenantID)
	r.mu.Unlock()
}

// dsnBuilderFromTemplate produces a DSNBuilder that substitutes {{tenant}}
// and {{password}} in template. The password comes from resolver on every
// invocation — caching belongs in the resolver, not here. Used internally
// when Config carries DSNTemplate + PasswordResolver instead of a raw
// DSNBuilder.
func dsnBuilderFromTemplate(template string, resolver PasswordResolver) func(string) (string, error) {
	return func(tenantID string) (string, error) {
		if tenantID == "" {
			return "", errors.New("tenant id is empty")
		}
		if !strings.Contains(template, "{{tenant}}") {
			return "", errors.New("DSNTemplate must contain {{tenant}}")
		}
		dsn := strings.ReplaceAll(template, "{{tenant}}", tenantID)

		if strings.Contains(dsn, "{{password}}") {
			if resolver == nil {
				return "", fmt.Errorf("DSNTemplate references {{password}} but no PasswordResolver was configured")
			}
			pw, err := resolver.Resolve(context.Background(), tenantID)
			if err != nil {
				return "", fmt.Errorf("resolve password for %s: %w", tenantID, err)
			}
			dsn = strings.ReplaceAll(dsn, "{{password}}", pw)
		}
		return dsn, nil
	}
}
