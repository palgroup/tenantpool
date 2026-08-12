package tenantpool_test

import (
	"os"
	"testing"
)

// TestDEL001_ResolutionMachineryGone locks the removal of the per-tenant
// resolution machinery. Each file existed only to answer "which tenant's
// pool, password or host" — a question a single-tenant stack never asks.
func TestDEL001_ResolutionMachineryGone(t *testing.T) {
	for _, name := range []string{
		"pooler_host_resolver.go",
		"password_resolver.go",
		"pooler_host_invalidation.go",
		"resolvers.go",
		"kv_resolver.go",
	} {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatalf("%s must not exist: multi-tenant resolution is gone in v2", name)
		}
	}
}
