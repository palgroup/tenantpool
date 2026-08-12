// Package tenantpool provides the Postgres connection pool for a Palbase
// stack.
//
// A stack serves one tenant and talks to one database, so there is one pool:
// opened at boot from one DSN, held for the life of the process, and reachable
// from anywhere through the context helpers. Nothing about an incoming request
// can select a different one — there is no second pool to select, and no
// accessor that takes a tenant identifier.
//
//	reg, err := tenantpool.New(tenantpool.Config{
//	    DatabaseURL: "postgres://user:pw@host:5432/db",
//	})
//	if err != nil {
//	    return err
//	}
//	defer reg.Close()
//
//	ctx = tenantpool.WithPool(ctx, reg.Pool())
//
// Handlers read the pool back out of the context:
//
//	pool := tenantpool.PoolFromCtx(r.Context())
package tenantpool

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config describes the one database this stack connects to.
type Config struct {
	// DatabaseURL is the DSN of the stack's Postgres. Pool tuning travels
	// inside it as pgxpool query parameters (pool_max_conns,
	// pool_min_conns, pool_max_conn_lifetime, …), which keeps the whole
	// connection target one string a deployment can set from one
	// environment variable.
	DatabaseURL string
}

// Registry owns the process's pool. Safe for concurrent use.
//
// Callers must call Close at service shutdown to drain it cleanly.
type Registry struct {
	pool *pgxpool.Pool
}

// New opens the pool from cfg.DatabaseURL. The pool is created eagerly so a
// malformed DSN fails the boot rather than the first request; pgxpool dials
// lazily, so an unreachable database still surfaces on first acquire.
//
// A missing or unparseable DatabaseURL returns ErrInvalidConfig.
func New(cfg Config) (*Registry, error) {
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("%w: DatabaseURL is required", ErrInvalidConfig)
	}
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("%w: parse dsn: %w", ErrInvalidConfig, err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUpstreamUnreachable, err)
	}
	return &Registry{pool: pool}, nil
}

// Pool returns the stack's pool.
//
// It takes no tenant identifier on purpose: the connection target is fixed at
// boot, so there is no request-borne value that could redirect a query to
// another tenant's database.
func (r *Registry) Pool() *pgxpool.Pool {
	return r.pool
}

// Close drains the pool. Call once at service shutdown; safe to call more than
// once.
func (r *Registry) Close() {
	r.pool.Close()
}
