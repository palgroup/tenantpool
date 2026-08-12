package tenantpool

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool must keep returning *pgxpool.Pool and must keep taking no arguments:
// the single-tenant stack opens one pool at boot, so a signature drifting back
// to Pool(tenantID) would be per-request resolution returning under a new name.
// Asserting the method set catches that; asserting one call's inferred type
// would not.
var _ interface{ Pool() *pgxpool.Pool } = (*Registry)(nil)

// registrySource reads registry.go so the DEL tests below can assert on
// identifiers that no longer exist. A compile-time assertion cannot express
// "this symbol is gone" — it would need the symbol to compile.
func registrySource(t *testing.T) []byte {
	t.Helper()
	src, err := os.ReadFile("registry.go")
	if err != nil {
		t.Fatal(err)
	}
	return src
}

// TestDEL012_ConfigRejectsDSNBuilder locks the removal of the per-tenant DSN
// path: Config no longer carries a builder, and Registry no longer has a
// tenant-keyed accessor to feed one. A single-tenant stack has exactly one
// database — nothing to choose between, and nothing a request could choose
// with.
//
// The kept side is asserted too, so emptying registry.go does not pass.
func TestDEL012_ConfigRejectsDSNBuilder(t *testing.T) {
	src := registrySource(t)
	for _, gone := range []string{
		"DSNBuilder",
		"func (r *Registry) Get(",
	} {
		if bytes.Contains(src, []byte(gone)) {
			t.Errorf("%q must be gone: the pool is fixed at boot, not built per tenant", gone)
		}
	}
	if kept := "func (r *Registry) Pool() *pgxpool.Pool"; !bytes.Contains(src, []byte(kept)) {
		t.Errorf("%q must stay: it is how callers reach the stack's single pool", kept)
	}
}

// TestDEL013_ConfigRejectsDSNTemplate locks the removal of the resolver-fed
// DSN path — a template plus the two resolvers that filled its holes (the
// tenant's password and the pooler host it lives behind). T002 removed the
// symbols; this test is what keeps them out.
func TestDEL013_ConfigRejectsDSNTemplate(t *testing.T) {
	src := registrySource(t)
	for _, gone := range []string{
		"DSNTemplate",
		"PasswordResolver",
		"PoolerHostResolver",
		"dsnBuilderFromTemplate",
	} {
		if bytes.Contains(src, []byte(gone)) {
			t.Errorf("%q must be gone: there is no per-tenant secret or host to resolve", gone)
		}
	}
}

// fakeDSN is parseable but unreachable. pgxpool dials lazily, so New can open
// a pool against it without a live Postgres; no test here acquires a
// connection.
const fakeDSN = "postgres://u:p@127.0.0.1:1/db?sslmode=disable"

func newRegistry(t *testing.T) *Registry {
	t.Helper()
	r, err := New(Config{DatabaseURL: fakeDSN})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(r.Close)
	return r
}

func TestNew_RequiresDatabaseURL(t *testing.T) {
	_, err := New(Config{})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

// TestNew_RejectsUnparseableDSN keeps the boot failing loudly: the DSN is the
// whole connection target now, so a bad one must not survive until the first
// query.
func TestNew_RejectsUnparseableDSN(t *testing.T) {
	_, err := New(Config{DatabaseURL: "postgres://u:p@127.0.0.1:notaport/db"})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

// TestPool_IsTheSamePoolOnEveryCall is the positive half of DEL-012: one stack,
// one pool, handed out unchanged however often it is asked for.
func TestPool_IsTheSamePoolOnEveryCall(t *testing.T) {
	r := newRegistry(t)

	pool := r.Pool()
	if pool == nil {
		t.Fatal("Pool() returned nil")
	}
	if r.Pool() != pool {
		t.Error("Pool() must return the same pool every call")
	}
}

// TestNew_AppliesPoolTuningFromDSN covers what shrinking Config to one field
// moved: sizing knobs that used to be Config fields now travel inside the DSN,
// and this proves they actually reach the pool.
func TestNew_AppliesPoolTuningFromDSN(t *testing.T) {
	r, err := New(Config{DatabaseURL: fakeDSN + "&pool_max_conns=7&pool_min_conns=2"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(r.Close)

	cfg := r.Pool().Config()
	if cfg.MaxConns != 7 {
		t.Errorf("MaxConns=%d, want 7 (pool_max_conns from the DSN)", cfg.MaxConns)
	}
	if cfg.MinConns != 2 {
		t.Errorf("MinConns=%d, want 2 (pool_min_conns from the DSN)", cfg.MinConns)
	}
}

func TestClose_IsIdempotent(t *testing.T) {
	r, err := New(Config{DatabaseURL: fakeDSN})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	r.Close()
	r.Close() // shutdown paths call Close from more than one place
}
