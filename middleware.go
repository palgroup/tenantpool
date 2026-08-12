package tenantpool

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// This file used to carry the per-request pool resolution path:
// Registry.Middleware read a tenant identifier off each request, asked the
// Registry for that tenant's pool, and attached it to the context. In a
// single-tenant stack there is nothing to resolve — the pool is opened at boot
// and never varies by request — so that path is gone (DEL-004).
//
// What survives is the context carriage itself: WithPool puts the pool in, the
// two PoolFromCtx forms take it back out. 457 call sites across eight modules
// read the pool this way, and they keep working: the pool now arrives from
// service wiring at startup rather than from a middleware that inspected the
// request.

type ctxKey struct{}

var poolKey = ctxKey{}

// PoolFromCtx extracts the pool attached with WithPool.
//
// Panics when no pool is present: a handler reaching here without one means the
// pool was never injected and silent failure would mask the bug. Callers that
// must tolerate a missing pool should use PoolFromCtxOK.
func PoolFromCtx(ctx context.Context) *pgxpool.Pool {
	p, ok := PoolFromCtxOK(ctx)
	if !ok {
		panic("tenantpool: no pool in context — was WithPool called during service startup?")
	}
	return p
}

// PoolFromCtxOK is the non-panicking form of PoolFromCtx.
func PoolFromCtxOK(ctx context.Context) (*pgxpool.Pool, bool) {
	p, ok := ctx.Value(poolKey).(*pgxpool.Pool)
	return p, ok
}

// WithPool returns a context with the given pool attached.
//
// Used wherever service code that reads PoolFromCtx has to be entered: HTTP
// server setup, and background workers (NATS consumers, Redis Streams
// consumers, cron jobs) that build their own root context.
func WithPool(ctx context.Context, pool *pgxpool.Pool) context.Context {
	return context.WithValue(ctx, poolKey, pool)
}
