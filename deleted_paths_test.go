package tenantpool_test

import (
	"os"
	"testing"
)

// The five tests below lock the removal of the per-tenant resolution machinery.
// Each file existed only to answer "which tenant's pool, password or host" — a
// question a single-tenant stack never asks.
//
// One test per DEL id, named exactly as DEL-index.json declares, because the
// mutation gate (FR-031) re-introduces one deleted path at a time and must be
// able to name the single test that should go red. A shared test covering all
// five would leave four ids with no test of their own.

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

func TestDEL030_TenantpoolHasNoKVResolver(t *testing.T) {
	absent(t, "kv_resolver.go")
}
