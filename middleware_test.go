package tenantpool

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddleware_SingleMode_AttachesPool(t *testing.T) {
	r, err := New(Config{DatabaseURL: "postgres://u:p@127.0.0.1:1/db?sslmode=disable"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var got bool
	h := r.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, ok := PoolFromCtxOK(req.Context())
		got = ok
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status=%d", rec.Code)
	}
	if !got {
		t.Error("handler did not see a pool in ctx")
	}
}

func TestMiddleware_MultiMode_UsesResolver(t *testing.T) {
	r, err := New(Config{
		DSNBuilder: fakeDSN,
		Resolver:   HeaderResolver("X-Tenant-Ref"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	h := r.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if _, ok := PoolFromCtxOK(req.Context()); !ok {
			t.Error("no pool in ctx")
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Tenant-Ref", "t1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status=%d", rec.Code)
	}
}

func TestMiddleware_MissingTenantHeader_Returns404(t *testing.T) {
	r, err := New(Config{
		DSNBuilder: fakeDSN,
		Resolver:   HeaderResolver("X-Tenant-Ref"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	h := r.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		t.Error("handler should not run")
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status=%d, want 404", rec.Code)
	}
}

func TestMiddleware_CustomErrorHandler(t *testing.T) {
	r, err := New(Config{
		DSNBuilder: fakeDSN,
		Resolver:   HeaderResolver("X-Tenant-Ref"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var gotErr error
	custom := func(w http.ResponseWriter, req *http.Request, err error) {
		gotErr = err
		http.Error(w, "custom", http.StatusTeapot)
	}

	h := r.Middleware(WithErrorHandler(custom))(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		t.Error("handler should not run on resolver error")
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTeapot {
		t.Errorf("status=%d, want 418", rec.Code)
	}
	if !errors.Is(gotErr, ErrTenantNotFound) {
		t.Errorf("captured err=%v, want ErrTenantNotFound", gotErr)
	}
}

func TestMiddleware_WithResolverOverride(t *testing.T) {
	r, err := New(Config{
		DSNBuilder: fakeDSN,
		Resolver:   HeaderResolver("X-Tenant-Ref"), // default
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Override: extract from query param instead.
	override := func(req *http.Request) (string, error) {
		if v := req.URL.Query().Get("tid"); v != "" {
			return v, nil
		}
		return "", ErrTenantNotFound
	}

	var seenTenant string
	h := r.Middleware(WithResolver(override))(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if _, ok := PoolFromCtxOK(req.Context()); ok {
			seenTenant = "ok"
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/?tid=t1", nil)
	// NB no X-Tenant-Ref header; default resolver would have failed.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status=%d", rec.Code)
	}
	if seenTenant != "ok" {
		t.Error("override resolver did not populate ctx")
	}
}

func TestMiddleware_MultiModeNoResolver_Errors(t *testing.T) {
	r, err := New(Config{
		DSNBuilder: fakeDSN,
		// no Resolver
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	h := r.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		t.Error("handler should not run")
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status=%d, want 500", rec.Code)
	}
}

func TestPoolFromCtx_PanicsWhenMissing(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic")
		}
	}()
	_ = PoolFromCtx(httptest.NewRequest("GET", "/", nil).Context())
}

func TestPoolFromCtxOK_FalseWhenMissing(t *testing.T) {
	_, ok := PoolFromCtxOK(httptest.NewRequest("GET", "/", nil).Context())
	if ok {
		t.Error("expected false")
	}
}

func TestWithPool_RoundTrip(t *testing.T) {
	r, err := New(Config{DatabaseURL: "postgres://u:p@127.0.0.1:1/db?sslmode=disable"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	pool, err := r.Get(httptest.NewRequest("GET", "/", nil).Context(), "ignored")
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithPool(httptest.NewRequest("GET", "/", nil).Context(), pool)
	if got, _ := PoolFromCtxOK(ctx); got != pool {
		t.Error("WithPool round-trip mismatch")
	}
}
