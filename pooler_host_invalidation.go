package tenantpool

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolerHostInvalidator is implemented by resolvers that can drop a cached
// tenant→pooler mapping. Optional on purpose: PoolerHostResolver is a one-method
// interface a caller can satisfy with a closure, and forcing every one of those
// to grow an Invalidate would buy nothing — a resolver with no cache has nothing
// to invalidate.
type PoolerHostInvalidator interface {
	Invalidate(tenantID string)
}

// Invalidate drops the cached host for one tenant so the next ResolveHost asks
// the orchestrator again.
//
// Deliberately narrow. The cache is also this resolver's FAIL-SAFE: when the
// orchestrator is unreachable, ResolveHost serves the stale host rather than
// refusing, because a control-plane blip must not take out data-plane traffic
// whose host has not changed. Invalidation must therefore fire ONLY on
// "the pooler I was sent to does not know this tenant" — never on "I could not
// reach the orchestrator". Those look alike from a distance, and collapsing them
// would turn every control-plane hiccup into a data-plane outage.
func (r *OrchestratorPoolerHostResolver) Invalidate(tenantID string) {
	r.mu.Lock()
	delete(r.cache, tenantID)
	r.mu.Unlock()
}

// isTenantUnknownToPooler reports whether a connection error means the pooler
// answered and disowned the tenant — as opposed to being unreachable, slow, or
// refusing the password.
//
// This is the exact failure of 2026-08-09: the registry healer pruned six pro
// tenants from the free pooler after routing moved, palsvc kept dialling the
// host it had cached for ten minutes, and every request failed with
//
//	FATAL: Tenant or user not found (SQLSTATE XX000)
//
// Supavisor reports it as a plain FATAL with SQLSTATE XX000 (internal_error),
// which it also uses for unrelated conditions — so the code alone cannot carry
// the decision and the message has to. That makes this string-sensitive, and a
// Supavisor reword would silently stop the invalidation.
//
// Which is why the test asserts against a *pgconn.PgError built to Supavisor's
// wire shape rather than a literal we could quietly keep passing: if this ever
// stops matching, what fails is a test, not a fleet.
func isTenantUnknownToPooler(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return isTenantUnknownPgError(pgErr)
}

// isTenantUnknownPgError is the same recognition against an already-unwrapped
// error, which is the shape the connection-level hook receives.
func isTenantUnknownPgError(pgErr *pgconn.PgError) bool {
	return pgErr != nil && pgErr.Message == "Tenant or user not found"
}

// verifyOrRepoint dials the freshly built pool once. On the one error that means
// the cached host is stale it invalidates, re-resolves and rebuilds; on anything
// else it keeps the pool exactly as before.
//
// Ping-then-keep, rather than ping-then-fail, is the whole point: pgxpool
// connects lazily, so before this the first symptom of a stale host was an error
// on a user's query, ten minutes long and self-inflicted. Verifying at
// acquisition moves that to the one place that can still do something about it.
// But a tenant database that is briefly down must NOT become a hard failure here
// — that would convert a transient into an outage — so every other ping error
// leaves today's behaviour untouched.
func (r *Registry) verifyOrRepoint(ctx context.Context, tenantID string, pool *pgxpool.Pool) *pgxpool.Pool {
	inv, ok := r.cfg.PoolerHostResolver.(PoolerHostInvalidator)
	if !ok {
		return pool
	}

	pingCtx, cancel := context.WithTimeout(ctx, poolerRepointPingTimeout)
	err := pool.Ping(pingCtx)
	cancel()
	if err == nil || !isTenantUnknownToPooler(err) {
		return pool
	}

	inv.Invalidate(tenantID)
	dsn, err := r.cfg.DSNBuilder(tenantID)
	if err != nil {
		return pool
	}
	repointed, err := r.buildPool(ctx, tenantID, dsn)
	if err != nil {
		return pool
	}
	pool.Close()
	return repointed
}

// onPgErrorFor wraps the connection's server-error handler so a pool that is
// ALREADY established reacts to being disowned.
//
// verifyOrRepoint covers only the pool it just built: Get's hot path returns a
// cached pool without checking anything, and it must — a Ping in front of every
// query would tax every tenant to protect against a rare event. So once a
// replica holds a pool, nothing re-examines it. When the healer prunes the
// tenant from that pooler, every query fails until the entry is evicted or the
// process restarts.
//
// That gap is measured, not theoretical: ~5 minutes of `Tenant or user not
// found` on 2026-08-09, ended by `kubectl rollout restart` — a restart being the
// only thing able to clear a cache nothing else could reach.
//
// This hook fires on a LIVE connection, which is what closes it. It is
// deliberately additive: pgx's verdict on whether to close the connection is
// left exactly as it was, and a caller's own handler still runs and still
// decides. The reaction runs off this goroutine because it closes the very pool
// this connection belongs to.
func (r *Registry) onPgErrorFor(tenantID string, prev pgconn.PgErrorHandler) pgconn.PgErrorHandler {
	return func(c *pgconn.PgConn, pgErr *pgconn.PgError) bool {
		if isTenantUnknownPgError(pgErr) {
			go r.repointAfterDisown(tenantID)
		}
		if prev != nil {
			return prev(c, pgErr)
		}
		// pgx's documented default: close on FATAL, keep otherwise.
		return !strings.EqualFold(pgErr.Severity, "FATAL")
	}
}

// repointAfterDisown drops both halves of the stale answer: the cached host, so
// the next resolve asks the orchestrator, and the pool built from it, so Get
// stops handing out connections to a pooler that has disowned this tenant.
//
// Dropping only the host would leave the pool cached and the outage running;
// dropping only the pool would rebuild it against the same stale host.
func (r *Registry) repointAfterDisown(tenantID string) {
	if inv, ok := r.cfg.PoolerHostResolver.(PoolerHostInvalidator); ok {
		inv.Invalidate(tenantID)
	}
	r.Invalidate(tenantID)
}

// poolerRepointPingTimeout bounds the acquisition-time check. Short because it
// sits in front of a caller's first query: a pooler that cannot answer a Ping in
// this long is not going to serve the query either, and the fallback is to keep
// the pool and let the caller see the real error.
const poolerRepointPingTimeout = 3 * time.Second
