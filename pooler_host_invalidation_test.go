package tenantpool

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
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
