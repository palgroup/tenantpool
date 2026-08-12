package tenantpool_test

import (
	"os"
	"testing"
)

// The tests below lock the removal of the per-tenant resolution machinery.
// Each file existed only to answer "which tenant's pool, password or host" — a
// question a single-tenant stack never asks.
//
// One test per DEL id, named exactly as DEL-index.json declares, because the
// mutation gate (FR-031) re-introduces one deleted path at a time and must be
// able to name the single test that should go red. A shared test covering
// several would leave the rest with no test of their own.

func absent(t *testing.T, name string) {
	t.Helper()
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Fatalf("%s must not exist: multi-tenant resolution is gone in v2", name)
	}
}

func TestDEL001_PoolerHostResolverAbsent(t *testing.T) {
	absent(t, "pooler_host_resolver.go")
}

func TestDEL002_PasswordResolverAbsent(t *testing.T) {
	absent(t, "password_resolver.go")
}

func TestDEL003_PoolerHostInvalidationAbsent(t *testing.T) {
	absent(t, "pooler_host_invalidation.go")
}

func TestDEL006_ResolverConstructorsAbsent(t *testing.T) {
	absent(t, "resolvers.go")
}

// TestDEL005_ResolutionMetricsAbsent locks the removal of the Prometheus
// bundle. Every collector in it counted a resolution event — pools created per
// tenant, pools evicted from the LRU cache, resolution errors by sentinel,
// acquire latency of the per-tenant Get. With one pool opened at boot there is
// no such event to count, and a live pool's own numbers come from
// pgxpool.Pool.Stat.
func TestDEL005_ResolutionMetricsAbsent(t *testing.T) {
	absent(t, "metrics.go")
}

// TestDEL014_SupavisorDSNBuilderAbsent locks the removal of the DSN builders.
// SupavisorDSN embedded the tenant id in the wire username so a tenant-aware
// pooler could route on it; DirectDSN/PgBouncerDSN made the tenant id the
// database name. All three answer "which tenant's database", and all three fed
// Config.DSNBuilder, which is gone with DEL-012.
func TestDEL014_SupavisorDSNBuilderAbsent(t *testing.T) {
	absent(t, "dsn.go")
}

func TestDEL030_TenantpoolHasNoKVResolver(t *testing.T) {
	absent(t, "kv_resolver.go")
}
