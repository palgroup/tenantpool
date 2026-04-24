package tenantpool

import (
	"context"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ctxKey struct{}

var poolKey = ctxKey{}

// ErrorHandler converts a Registry error into an HTTP response.
// Services with a canonical error envelope should plug it in through
// WithErrorHandler; otherwise DefaultErrorHandler is used.
type ErrorHandler func(w http.ResponseWriter, r *http.Request, err error)

// Resolver extracts the tenant ID from an HTTP request. The default
// Middleware skips resolution in single-database mode.
type Resolver func(r *http.Request) (tenantID string, err error)

// Middleware returns the HTTP middleware that attaches a pool to the
// request context. Handlers read it back with PoolFromCtx.
//
// In single-database mode the configured pool is attached directly,
// skipping resolver and DSN builder. In multi-tenant mode the
// Registry's Resolver is called to identify the tenant; a missing or
// errorful resolver aborts the chain via the error handler.
func (r *Registry) Middleware(opts ...MiddlewareOption) func(http.Handler) http.Handler {
	mo := middlewareOptions{onErr: DefaultErrorHandler}
	for _, opt := range opts {
		opt(&mo)
	}
	// In multi-tenant mode a resolver is mandatory for HTTP requests;
	// callers that only use the Registry from background workers can
	// rely on Get + WithPool directly and skip Middleware.
	resolver := r.cfg.Resolver
	if mo.resolver != nil {
		resolver = mo.resolver
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if r.single != nil {
				ctx := WithPool(req.Context(), r.single)
				next.ServeHTTP(w, req.WithContext(ctx))
				return
			}
			if resolver == nil {
				mo.onErr(w, req, errors.New("tenantpool: Middleware used in multi-tenant mode without a Resolver"))
				return
			}
			tenantID, err := resolver(req)
			if err != nil {
				mo.onErr(w, req, err)
				return
			}
			pool, err := r.Get(req.Context(), tenantID)
			if err != nil {
				mo.onErr(w, req, err)
				return
			}
			ctx := WithPool(req.Context(), pool)
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	}
}

type middlewareOptions struct {
	onErr    ErrorHandler
	resolver Resolver
}

// MiddlewareOption tweaks the behaviour of Registry.Middleware.
type MiddlewareOption func(*middlewareOptions)

// WithErrorHandler installs a custom error handler, typically one that
// writes the service's canonical JSON envelope. Without it,
// DefaultErrorHandler writes a plain 503.
func WithErrorHandler(h ErrorHandler) MiddlewareOption {
	return func(o *middlewareOptions) { o.onErr = h }
}

// WithResolver overrides the Registry's Config.Resolver for this
// Middleware instance. Useful when the same Registry is shared between
// routers that identify tenants differently (e.g. public vs admin
// routes keyed off different headers).
func WithResolver(res Resolver) MiddlewareOption {
	return func(o *middlewareOptions) { o.resolver = res }
}

// DefaultErrorHandler writes a plain text 503 response. Services with a
// canonical error envelope should replace this via WithErrorHandler.
func DefaultErrorHandler(w http.ResponseWriter, _ *http.Request, err error) {
	switch {
	case errors.Is(err, ErrTenantNotFound):
		http.Error(w, `{"error":"tenant_not_found"}`, http.StatusNotFound)
	case errors.Is(err, ErrPoolExhausted), errors.Is(err, ErrUpstreamUnreachable):
		http.Error(w, `{"error":"database_unavailable"}`, http.StatusServiceUnavailable)
	default:
		http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
	}
}

// PoolFromCtx extracts the pool attached by Middleware.
//
// Panics when no pool is present: a handler reaching here without one
// means the middleware chain is misconfigured and silent failure would
// mask the bug. Callers that must tolerate a missing pool should use
// PoolFromCtxOK.
func PoolFromCtx(ctx context.Context) *pgxpool.Pool {
	p, ok := PoolFromCtxOK(ctx)
	if !ok {
		panic("tenantpool: no pool in context — is Registry.Middleware wired?")
	}
	return p
}

// PoolFromCtxOK is the non-panicking form of PoolFromCtx.
func PoolFromCtxOK(ctx context.Context) (*pgxpool.Pool, bool) {
	p, ok := ctx.Value(poolKey).(*pgxpool.Pool)
	return p, ok
}

// WithPool returns a context with the given pool attached.
//
// Used by background workers (NATS consumers, Redis Streams consumers,
// cron jobs) that resolve the tenant from message payloads rather than
// HTTP headers: Registry.Get to fetch the pool, WithPool to attach it,
// then call into service code that expects PoolFromCtx.
func WithPool(ctx context.Context, pool *pgxpool.Pool) context.Context {
	return context.WithValue(ctx, poolKey, pool)
}
