package tenantpool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestIsTenantUnknownToPooler_matchesSupavisorsRealError pins the recogniser
// against the wire shape Supavisor actually produces, not a literal we control.
//
// The distinction it has to draw is the whole safety argument: "the pooler
// disowned this tenant" invalidates a cached host, while "the pooler is
// unreachable" must NOT — the cache is the fail-safe that keeps data-plane
// traffic alive through a control-plane blip. A recogniser that is too broad
// turns every hiccup into a re-resolve storm; too narrow and the ten-minute
// outage of 2026-08-09 repeats.
func TestIsTenantUnknownToPooler_matchesSupavisorsRealError(t *testing.T) {
	t.Parallel()

	// Supavisor's actual response when routing has moved and the registry
	// healer has pruned the tenant from this pooler.
	stale := &pgconn.PgError{
		Severity: "FATAL",
		Code:     "XX000",
		Message:  "Tenant or user not found",
	}
	if !isTenantUnknownToPooler(stale) {
		t.Fatal("the error that caused the outage must be recognised")
	}
	// Wrapped is the shape it actually arrives in, through pgxpool.
	if !isTenantUnknownToPooler(errors.Join(errors.New("dial"), stale)) {
		t.Error("must survive wrapping — this never arrives bare")
	}

	// Everything else must be left alone.
	for _, other := range []error{
		errors.New("dial tcp: i/o timeout"),
		&pgconn.PgError{Severity: "FATAL", Code: "28P01", Message: "password authentication failed"},
		// Same SQLSTATE, different condition: XX000 is Supavisor's catch-all,
		// so the code alone must never be enough to invalidate.
		&pgconn.PgError{Severity: "FATAL", Code: "XX000", Message: "temporarily unavailable"},
		nil,
	} {
		if isTenantUnknownToPooler(other) {
			t.Errorf("must not invalidate on: %v", other)
		}
	}
}

// TestInvalidateDropsOnlyTheNamedTenant pins that invalidation is surgical. The
// cache is shared across every tenant a process serves; clearing it wholesale on
// one tenant's stale host would send every other tenant back to the orchestrator
// at once, which is a thundering herd aimed at the control plane during exactly
// the incident that triggered it.
func TestInvalidateDropsOnlyTheNamedTenant(t *testing.T) {
	t.Parallel()
	r := NewOrchestratorPoolerHostResolver("http://orchestrator.invalid", "secret", 0)
	r.cache["a"] = poolerHostEntry{host: "pooler-1"}
	r.cache["b"] = poolerHostEntry{host: "pooler-2"}

	r.Invalidate("a")

	if _, ok := r.cache["a"]; ok {
		t.Error("the named tenant's entry must be dropped")
	}
	if _, ok := r.cache["b"]; !ok {
		t.Error("another tenant's entry must survive — one stale host is not a reason to stampede the control plane")
	}
}

// TestOrchestratorResolverImplementsInvalidator keeps the optional interface
// actually satisfied. It is optional so closure-based resolvers stay valid, and
// the cost of optional is that dropping the method compiles fine and silently
// disables every repoint.
func TestOrchestratorResolverImplementsInvalidator(t *testing.T) {
	t.Parallel()
	var _ PoolerHostInvalidator = (*OrchestratorPoolerHostResolver)(nil)
}

// TestOnPgError_repointsAnALREADYESTABLISHEDPool is the half of the outage that
// verifyOrRepoint does not reach.
//
// verifyOrRepoint runs once, inside Get's singleflight, on a pool it just built.
// Get's hot path returns a CACHED pool without verifying anything — correctly,
// because verifying per call would put a Ping in front of every query. So a
// replica that already holds a pool keeps handing it out after the healer prunes
// the tenant from that pooler, and every query fails until the entry is evicted
// or the process restarts.
//
// That is not hypothetical: it is the ~5 minutes of `Tenant or user not found`
// on 2026-08-09, recovered by `kubectl rollout restart` — a restart being the
// only thing that could clear a cache nothing else could reach.
//
// The server-error hook closes it, because it fires on a LIVE connection rather
// than at acquisition.
func TestOnPgError_repointsAnAlreadyEstablishedPool(t *testing.T) {
	t.Parallel()

	res := &recordingInvalidator{}
	r := &Registry{
		cfg:   Config{PoolerHostResolver: res, MaxPoolsCached: 8},
		cache: map[string]*poolEntry{"todoappm8p6zm": {pool: lazyPool(t)}},
	}

	handler := r.onPgErrorFor("todoappm8p6zm", nil)
	disowned := &pgconn.PgError{Severity: "FATAL", Code: "XX000", Message: "Tenant or user not found"}

	if keepOpen := handler(nil, disowned); keepOpen {
		t.Error("a FATAL must still close the connection — the hook adds a reaction, it does not change pgx's verdict")
	}
	res.waitFor(t, "todoappm8p6zm")

	if r.cached("todoappm8p6zm") {
		t.Fatal("the pool built against the stale pooler must be dropped, or Get keeps handing it out")
	}
}

// The hook must react to exactly one condition. A tenant database that is down,
// or a password that is wrong, must not evict a pool and re-resolve — that turns
// a transient into a stampede at the control plane.
func TestOnPgError_leavesEveryOtherErrorAlone(t *testing.T) {
	t.Parallel()

	for _, other := range []*pgconn.PgError{
		{Severity: "FATAL", Code: "28P01", Message: "password authentication failed"},
		{Severity: "FATAL", Code: "XX000", Message: "temporarily unavailable"},
		{Severity: "ERROR", Code: "42P01", Message: "relation \"todos\" does not exist"},
	} {
		res := &recordingInvalidator{}
		r := &Registry{
			cfg:   Config{PoolerHostResolver: res, MaxPoolsCached: 8},
			cache: map[string]*poolEntry{"todoappm8p6zm": {pool: lazyPool(t)}},
		}
		r.onPgErrorFor("todoappm8p6zm", nil)(nil, other)

		// The reaction fires on its own goroutine, so absence has to be given a
		// chance to happen before it can be asserted — checking immediately
		// would pass no matter what the recogniser did.
		time.Sleep(50 * time.Millisecond)

		if res.calledFor("todoappm8p6zm") {
			t.Errorf("must not invalidate the pooler host on: %s", other.Message)
		}
		if !r.cached("todoappm8p6zm") {
			t.Errorf("must not drop the pool on: %s", other.Message)
		}
	}
}

// A caller's own OnPgError (set through ConfigurePool) must keep running and
// keep deciding. The hook is additive: wrapping it away would silently disable
// whatever the caller installed it for.
func TestOnPgError_preservesTheCallersHandler(t *testing.T) {
	t.Parallel()

	res := &recordingInvalidator{}
	r := &Registry{
		cfg:   Config{PoolerHostResolver: res, MaxPoolsCached: 8},
		cache: map[string]*poolEntry{},
	}

	var sawPrev bool
	prev := func(_ *pgconn.PgConn, _ *pgconn.PgError) bool { sawPrev = true; return true }

	keepOpen := r.onPgErrorFor("todoappm8p6zm", prev)(nil,
		&pgconn.PgError{Severity: "FATAL", Code: "XX000", Message: "Tenant or user not found"})

	if !sawPrev {
		t.Error("the caller's handler must still be called")
	}
	if !keepOpen {
		t.Error("and its verdict must win — it is the one the caller configured")
	}
}

// recordingInvalidator is a PoolerHostResolver that records which tenants were
// invalidated. The hook fires the invalidation off the connection goroutine, so
// waitFor gives it a bounded moment rather than racing it.
type recordingInvalidator struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (r *recordingInvalidator) ResolveHost(context.Context, string) (string, error) {
	return "supavisor-free.data-pool.svc.cluster.local", nil
}

func (r *recordingInvalidator) Invalidate(tenantID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen == nil {
		r.seen = map[string]bool{}
	}
	r.seen[tenantID] = true
}

func (r *recordingInvalidator) calledFor(tenantID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[tenantID]
}

func (r *recordingInvalidator) waitFor(t *testing.T, tenantID string) {
	t.Helper()
	for range 100 {
		if r.calledFor(tenantID) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("pooler host for %s was never invalidated", tenantID)
}

// lazyPool builds a pgxpool that never dials: MinConns 0 means pgxpool connects
// on first acquire, and these tests never acquire. It exists so the cache holds
// the same shape production does — Registry.Invalidate closes what it evicts.
func lazyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cfg.MinConns = 0
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	return p
}

// cached reads the pool cache under the registry's own lock, because the
// reaction under test writes it from another goroutine.
func (r *Registry) cached(tenantID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.cache[tenantID]
	return ok
}

// A hook nobody installs is dead code, and this repo has already paid for that
// once: e97dcb1 exists solely because verifyOrRepoint was written, reviewed and
// merged without Get ever calling it. The behaviour tests above drive
// onPgErrorFor directly, so they would all still pass with the wiring removed —
// this one fails.
//
// Asserted against a pool pgx actually built, not against the source text: what
// matters is that a connection ends up carrying the handler.
func TestBuildPool_installsTheDisownHookOnTheConnection(t *testing.T) {
	t.Parallel()

	r := &Registry{cfg: Config{
		PoolerHostResolver: &recordingInvalidator{},
		MaxPoolsCached:     8,
		MaxConnsPerPool:    1,
	}}

	pool, err := r.buildPool(context.Background(), "todoappm8p6zm",
		"postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("buildPool: %v", err)
	}
	defer pool.Close()

	if pool.Config().ConnConfig.OnPgError == nil {
		t.Fatal("connections must carry the disown hook, or a live pool can never repoint")
	}

	// Firing it must reach the reaction — proving the installed handler is ours
	// and not pgx's default.
	res := r.cfg.PoolerHostResolver.(*recordingInvalidator)
	pool.Config().ConnConfig.OnPgError(nil,
		&pgconn.PgError{Severity: "FATAL", Code: "XX000", Message: "Tenant or user not found"})
	res.waitFor(t, "todoappm8p6zm")
}

// Single-tenant mode has no tenant to repoint, so it must not install a hook
// that would name the empty string.
func TestBuildPool_singleModeInstallsNoHook(t *testing.T) {
	t.Parallel()

	r := &Registry{cfg: Config{MaxConnsPerPool: 1}}
	pool, err := r.buildPool(context.Background(), "", "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("buildPool: %v", err)
	}
	defer pool.Close()

	if pool.Config().ConnConfig.OnPgError == nil {
		t.Fatal("pgx's own default must remain in place")
	}
	if got := pool.Config().ConnConfig.OnPgError(nil,
		&pgconn.PgError{Severity: "FATAL", Code: "XX000", Message: "Tenant or user not found"}); got {
		t.Error("pgx's default closes a FATAL connection; single mode must not change that")
	}
}
