package tenantpool

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// staticHost returns a PoolerHostResolver that always resolves to h.
func staticHost(h string) PoolerHostResolver {
	return PoolerHostResolverFunc(func(context.Context, string) (string, error) { return h, nil })
}

func TestDSNBuilderFromTemplate_SubstitutesPooler(t *testing.T) {
	t.Parallel()
	b := dsnBuilderFromTemplate(
		"postgres://u_{{tenant}}.{{tenant}}:{{password}}@{{pooler}}:5432/{{tenant}}?sslmode=disable",
		StaticPasswordResolver("pw"),
		staticHost("supavisor-cell-pro-northeurope-02.data-pool.svc.cluster.local"),
	)
	dsn, err := b("todoappm8p6zm")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	want := "@supavisor-cell-pro-northeurope-02.data-pool.svc.cluster.local:5432/"
	if !strings.Contains(dsn, want) {
		t.Fatalf("dsn %q does not contain %q", dsn, want)
	}
}

// A template WITHOUT {{pooler}} keeps working untouched: the migration is
// per-service, and a half-migrated fleet must not break.
func TestDSNBuilderFromTemplate_NoPoolerPlaceholderNeedsNoResolver(t *testing.T) {
	t.Parallel()
	b := dsnBuilderFromTemplate(
		"postgres://u_{{tenant}}.{{tenant}}:{{password}}@supavisor-free.data-pool.svc.cluster.local:5432/{{tenant}}",
		StaticPasswordResolver("pw"), nil)
	dsn, err := b("todoappm8p6zm")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !strings.Contains(dsn, "@supavisor-free.") {
		t.Fatalf("dsn %q does not contain %q", dsn, "@supavisor-free.")
	}
}

// {{pooler}} with no resolver must FAIL LOUDLY, never silently emit a
// template literal (or a hardcoded default pool) as a hostname.
func TestDSNBuilderFromTemplate_PoolerWithoutResolverErrors(t *testing.T) {
	t.Parallel()
	b := dsnBuilderFromTemplate("postgres://x@{{pooler}}:5432/{{tenant}}", nil, nil)
	_, err := b("todoappm8p6zm")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "{{pooler}}") {
		t.Fatalf("error %q does not mention {{pooler}}", err.Error())
	}
}

// The resolver is asked per TENANT, because two tenants on one service can
// live in different cells — the exact condition that made a single literal
// host impossible to choose. Repeat calls for the same tenant within TTL
// must not re-hit the orchestrator.
func TestOrchestratorPoolerHostResolver_PerTenantAndCached(t *testing.T) {
	t.Parallel()
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		ref := path.Base(r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]string{"host": "supavisor-" + ref + ".test"})
	}))
	defer srv.Close()

	r := NewOrchestratorPoolerHostResolver(srv.URL, "secret", time.Minute)
	for i := 0; i < 3; i++ {
		h, err := r.ResolveHost(context.Background(), "a")
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if h != "supavisor-a.test" {
			t.Fatalf("call %d: got %q", i, h)
		}
	}
	hb, err := r.ResolveHost(context.Background(), "b")
	if err != nil {
		t.Fatalf("tenant b: %v", err)
	}
	if hb != "supavisor-b.test" {
		t.Fatalf("tenant b: got %q", hb)
	}
	if c := atomic.LoadInt32(&calls); c != 2 {
		t.Fatalf("orchestrator called %d times, want 2 (one per tenant, then cached)", c)
	}
}

// An unreachable orchestrator must not break a tenant whose host is already
// known — fail-safe, not fail-open. This is the requirement the whole
// resolver exists to satisfy: a routing hint that keeps serving the last
// known answer rather than tearing down a working connection path.
func TestOrchestratorPoolerHostResolver_ServesLastKnownOnError(t *testing.T) {
	t.Parallel()
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"host": "supavisor-a.test"})
	}))
	defer srv.Close()

	r := NewOrchestratorPoolerHostResolver(srv.URL, "secret", time.Nanosecond)
	first, err := r.ResolveHost(context.Background(), "a")
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}

	fail.Store(true)
	again, err := r.ResolveHost(context.Background(), "a")
	if err != nil {
		t.Fatalf("a failed refresh must not fail the caller: %v", err)
	}
	if again != first {
		t.Fatalf("got %q, want last known host %q", again, first)
	}
}

// A cold cache (no prior successful resolve for this tenant) has nothing to
// fall back to, so a failing orchestrator MUST surface as an error rather
// than inventing a host. This is the explicit, tested boundary of the
// fail-safe behaviour above — fail-safe means "serve what you already
// proved works", never "guess".
func TestOrchestratorPoolerHostResolver_ColdCacheWithFailingResolverErrors(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	r := NewOrchestratorPoolerHostResolver(srv.URL, "secret", time.Minute)
	if _, err := r.ResolveHost(context.Background(), "never-seen"); err == nil {
		t.Fatal("expected error: no cached host and orchestrator unreachable")
	}
}

// signPoolerHostRequest's canonical message MUST stay byte-for-byte
// identical to the orchestrator verifier (and to
// modules/backend/internal/internalsig.ComputeV2, which the orchestrator's
// br-pod/broker callers already share). This golden signature was computed
// independently (Python hmac/hashlib, not this package) from the fixed
// inputs below — it locks the wire format itself, not just this
// implementation's self-consistency.
func TestSignPoolerHostRequest_MatchesIndependentGolden(t *testing.T) {
	t.Parallel()
	req, err := http.NewRequest(http.MethodGet, "http://x/internal/backend/resolve-pooler-host/todoappm8p6zm", nil)
	if err != nil {
		t.Fatal(err)
	}
	fixedNow := time.Unix(1700000000, 0).UTC()
	signPoolerHostRequest(req, "test-secret", "todoappm8p6zm", "0123456789abcdef0123456789abcdef", fixedNow)

	const wantSig = "8da4e8fa27b71b24545504d4325e32014f56b390f33f7a71212721d87d2e5bfe"
	if got := req.Header.Get(poolerHostHeaderSignature); got != wantSig {
		t.Fatalf("signature = %q, want %q", got, wantSig)
	}
	if got := req.Header.Get(poolerHostHeaderSigVersion); got != "v2" {
		t.Fatalf("Signature-Version = %q, want v2", got)
	}
	if got := req.Header.Get(poolerHostHeaderSigCaller); got != "control-plane" {
		t.Fatalf("Signature-Caller = %q, want control-plane", got)
	}
	if got := req.Header.Get(poolerHostHeaderTimestamp); got != "1700000000" {
		t.Fatalf("X-Internal-Timestamp = %q, want 1700000000", got)
	}
	if got := req.Header.Get(poolerHostHeaderNonce); got != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("X-Internal-Nonce = %q, want the fixed nonce", got)
	}
}

func TestOrchestratorPoolerHostResolver_SignsEveryRequest(t *testing.T) {
	t.Parallel()
	var gotVersion, gotCaller string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotVersion = r.Header.Get(poolerHostHeaderSigVersion)
		gotCaller = r.Header.Get(poolerHostHeaderSigCaller)
		if r.Header.Get(poolerHostHeaderSignature) == "" || r.Header.Get(poolerHostHeaderNonce) == "" || r.Header.Get(poolerHostHeaderTimestamp) == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"host": "supavisor-a.test"})
	}))
	defer srv.Close()

	r := NewOrchestratorPoolerHostResolver(srv.URL, "secret", time.Minute)
	if _, err := r.ResolveHost(context.Background(), "a"); err != nil {
		t.Fatalf("ResolveHost: %v", err)
	}
	if gotVersion != "v2" {
		t.Fatalf("Signature-Version = %q, want v2", gotVersion)
	}
	if gotCaller != "control-plane" {
		t.Fatalf("Signature-Caller = %q, want control-plane", gotCaller)
	}
}
