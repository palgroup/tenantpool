package tenantpool

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PoolerHostResolver answers which Supavisor a tenant's connections go through.
//
// Per tenant, not per process: tenants are spread across cells and each cell
// has its own pooler, so no single literal host is correct for a
// multi-tenant service. A service that hardcoded one was left dialling the
// old pool when the routing moved, and its tenants failed with "Tenant or
// user not found".
type PoolerHostResolver interface {
	ResolveHost(ctx context.Context, tenantID string) (string, error)
}

// PoolerHostResolverFunc adapts a plain function into a PoolerHostResolver.
// Use it for one-off test stubs.
type PoolerHostResolverFunc func(ctx context.Context, tenantID string) (string, error)

// ResolveHost implements PoolerHostResolver.
func (f PoolerHostResolverFunc) ResolveHost(ctx context.Context, tenantID string) (string, error) {
	return f(ctx, tenantID)
}

// resolvePoolerHostPath is the orchestrator route this resolver calls. It
// MUST match the orchestrator mount (httpserver/server.go) and the
// parseTargetRef binding (httpserver/auth.go) exactly — the ref is the path
// tail, bound into the v2 signature so a captured signature can't be
// replayed across refs.
const resolvePoolerHostPath = "/internal/backend/resolve-pooler-host/"

// OrchestratorPoolerHostResolver reads the host from the orchestrator, the
// one authority for tenant→cell→pooler, and caches it per tenant for ttl.
//
// The cached value is also the FALLBACK: a failed refresh serves the last
// known host rather than failing the caller. An unreachable control plane
// must not take out data-plane traffic that is working — this resolver
// returns a routing hint, not an authorization decision, so it fails safe
// (keep serving) rather than fail open/closed the way a security gate would.
type OrchestratorPoolerHostResolver struct {
	baseURL string
	secret  string
	ttl     time.Duration
	client  *http.Client

	mu    sync.Mutex
	cache map[string]poolerHostEntry
}

type poolerHostEntry struct {
	host      string
	fetchedAt time.Time
}

// NewOrchestratorPoolerHostResolver builds a resolver that resolves per-tenant
// pooler hosts through the orchestrator. baseURL is the orchestrator's
// internal HTTP base (no trailing slash needed); controlPlaneSecret is
// identity.DeriveControlPlaneSecret(fleetRoot) — the caller derives it, this
// resolver only signs with it; ttl is the resolved-host cache lifetime
// (<=0 defaults to 600s).
func NewOrchestratorPoolerHostResolver(baseURL, controlPlaneSecret string, ttl time.Duration) *OrchestratorPoolerHostResolver {
	if ttl <= 0 {
		ttl = 600 * time.Second
	}
	return &OrchestratorPoolerHostResolver{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		secret:  controlPlaneSecret,
		ttl:     ttl,
		client:  &http.Client{Timeout: 5 * time.Second},
		cache:   map[string]poolerHostEntry{},
	}
}

// ResolveHost returns the cached host while it is fresh, refreshes it when
// the TTL has passed, and on a refresh failure returns the STALE host rather
// than an error — a control plane that is briefly unreachable must not take
// down data-plane traffic whose host has not changed. Only a tenant with no
// cached host at all (cold cache) can fail — there is nothing to fall back
// to.
func (r *OrchestratorPoolerHostResolver) ResolveHost(ctx context.Context, tenantID string) (string, error) {
	if tenantID == "" {
		return "", errors.New("tenantpool: tenant id is empty")
	}

	r.mu.Lock()
	entry, cached := r.cache[tenantID]
	r.mu.Unlock()
	if cached && time.Since(entry.fetchedAt) < r.ttl {
		return entry.host, nil
	}

	host, err := r.fetchHost(ctx, tenantID)
	if err != nil {
		if cached {
			// Fail-safe, not fail-open: an unreachable orchestrator is
			// exactly when live traffic most needs the pool it already has.
			// This is a routing hint, so serving stale beats refusing a
			// working connection path.
			return entry.host, nil
		}
		return "", err
	}

	r.mu.Lock()
	r.cache[tenantID] = poolerHostEntry{host: host, fetchedAt: time.Now()}
	r.mu.Unlock()
	return host, nil
}

// fetchHost makes the control-plane-signed GET to the orchestrator and
// returns the resolved pooler host.
func (r *OrchestratorPoolerHostResolver) fetchHost(ctx context.Context, tenantID string) (string, error) {
	url := r.baseURL + resolvePoolerHostPath + tenantID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("build resolve-pooler-host request for %q: %w", tenantID, err)
	}

	nonce, err := poolerHostNonce()
	if err != nil {
		return "", fmt.Errorf("nonce for %q: %w", tenantID, err)
	}
	signPoolerHostRequest(req, r.secret, tenantID, nonce, time.Now())

	resp, err := r.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("resolve pooler host for %q via orchestrator: %w", tenantID, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("orchestrator resolve pooler host for %q: status %d: %s", tenantID, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out struct {
		Host string `json:"host"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&out); err != nil {
		return "", fmt.Errorf("decode pooler host response for %q: %w", tenantID, err)
	}
	if out.Host == "" {
		return "", fmt.Errorf("orchestrator returned empty pooler host for %q", tenantID)
	}
	return out.Host, nil
}

// The header names/values and the v2 message format below mirror
// modules/backend/internal/internalsig.SignV2 byte-for-byte — that package
// is internal/ to the backend-runtime module and cannot be imported from
// this separate tenantpool module. This is the same constraint (and the
// same fix) documented on modules/common/identity's duplicated HKDF
// derivation: a second copy, pinned by shared behavior rather than a shared
// import. If this canonicalization ever drifts from internalsig's, every
// call through this resolver starts getting 401s from the orchestrator.
const (
	poolerHostHeaderTimestamp  = "X-Internal-Timestamp"
	poolerHostHeaderSignature  = "X-Internal-Signature"
	poolerHostHeaderNonce      = "X-Internal-Nonce"
	poolerHostHeaderSigVersion = "Signature-Version"
	poolerHostHeaderSigCaller  = "Signature-Caller"

	poolerHostSigVersion = "v2"

	// poolerHostSigCallerControlPlane is the Signature-Caller value for a
	// control-plane caller — the orchestrator does NOT enforce caller==ref
	// for it, and verifies with DeriveControlPlaneSecret rather than a
	// per-ref key. Every client resolving a pooler host legitimately
	// resolves ANY ref, so it signs as control-plane, exactly like the
	// capability-broker's resolve-runtime-dsn caller.
	poolerHostSigCallerControlPlane = "control-plane"
)

// poolerHostNonce returns a fresh 128-bit random nonce as lowercase hex.
// crypto/rand is mandatory — the nonce is the per-request replay token and a
// predictable value would let an attacker pre-seed the orchestrator's replay
// cache.
func poolerHostNonce() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// signPoolerHostRequest attaches the v2 HMAC headers the orchestrator's
// resolve-pooler-host route verifies. req carries no query string and no
// body (GET), so the canonical message hashes an empty body and an empty
// query — see internalsig.ComputeV2 for the general form this specializes.
func signPoolerHostRequest(req *http.Request, secret, ref, nonce string, now time.Time) {
	ts := now.Unix()
	bodyHash := sha256.Sum256(nil)
	msg := strings.Join([]string{
		poolerHostSigVersion,
		req.Method,
		req.URL.Path,
		"", // no query string on this call
		ref,
		strconv.FormatInt(ts, 10),
		nonce,
		hex.EncodeToString(bodyHash[:]),
	}, "\n")
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(msg))

	req.Header.Set(poolerHostHeaderSigVersion, poolerHostSigVersion)
	req.Header.Set(poolerHostHeaderSigCaller, poolerHostSigCallerControlPlane)
	req.Header.Set(poolerHostHeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(poolerHostHeaderNonce, nonce)
	req.Header.Set(poolerHostHeaderSignature, hex.EncodeToString(mac.Sum(nil)))
}
