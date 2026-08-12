package tenantpool

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The Middleware tests that used to live here went with Registry.Middleware
// itself (DEL-004), and the file-local headerResolver they shared went with
// them: every one of them asserted that a tenant identifier taken off a request
// selected the pool, which is precisely the behaviour a single-tenant stack must
// not have. What is left tests the context carriage the helpers still provide.

func TestPoolFromCtx_PanicsWhenMissing(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic")
		}
	}()
	_ = PoolFromCtx(context.Background())
}

func TestPoolFromCtxOK_FalseWhenMissing(t *testing.T) {
	_, ok := PoolFromCtxOK(context.Background())
	if ok {
		t.Error("expected false")
	}
}

// TestWithPool_RoundTrip locks the surviving half of DEL-004: the three ctx
// helpers 457 call sites depend on. It builds the pool directly instead of
// through the Registry so the lock keeps holding when the Registry's own
// accessor changes shape.
func TestWithPool_RoundTrip(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	ctx := WithPool(context.Background(), pool)
	if got := PoolFromCtx(ctx); got != pool {
		t.Error("PoolFromCtx did not return the pool WithPool attached")
	}
	got, ok := PoolFromCtxOK(ctx)
	if !ok || got != pool {
		t.Errorf("PoolFromCtxOK = (%v, %v), want the attached pool and true", got, ok)
	}
}

// TestDEL004_RegistryHasNoRequestMiddleware locks that the pool is no longer
// resolved per request: in a single-tenant stack the connection is fixed at
// boot, so nothing about an incoming request may pick it.
//
// The test reads the source rather than the symbol table because the point is
// that these identifiers no longer exist to be referenced — a compile-time
// assertion would need them to compile. It also asserts the KEPT side of the
// same inventory entry, so deleting middleware.go outright fails too.
func TestDEL004_RegistryHasNoRequestMiddleware(t *testing.T) {
	src, err := os.ReadFile("middleware.go")
	if err != nil {
		t.Fatal(err)
	}
	// Exactly the symbols DEL-004 scopes: the per-request resolution path.
	for _, gone := range []string{
		"func (r *Registry) Middleware(",
		"MiddlewareOption",
		"middlewareOptions",
		"func WithResolver(",
		"func WithErrorHandler(",
		"type ErrorHandler ",
		"type Resolver ",
		"func DefaultErrorHandler(",
	} {
		if bytes.Contains(src, []byte(gone)) {
			t.Errorf("%q must be gone: the pool is chosen at boot, not per request", gone)
		}
	}
	// The helpers survive — 457 call sites across eight modules read them.
	for _, kept := range []string{
		"func PoolFromCtx(",
		"func PoolFromCtxOK(",
		"func WithPool(",
	} {
		if !bytes.Contains(src, []byte(kept)) {
			t.Errorf("%q must stay: 457 call sites depend on it", kept)
		}
	}
}
