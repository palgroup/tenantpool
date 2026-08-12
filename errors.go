package tenantpool

import "errors"

// Sentinel errors returned by Registry operations. Use errors.Is to detect
// them; services map them to HTTP responses as they see fit.
var (
	// ErrInvalidConfig is returned by New when DatabaseURL is missing or
	// cannot be parsed. Treat as a programming or deployment error; 500.
	ErrInvalidConfig = errors.New("tenantpool: invalid config")

	// ErrTenantNotFound has no producer left in this package: it was
	// returned when a per-tenant DSN could not be resolved, and a
	// single-tenant stack resolves nothing. Kept because consuming
	// modules still map it.
	ErrTenantNotFound = errors.New("tenantpool: tenant not found")

	// ErrUpstreamUnreachable is returned when the pool cannot be opened
	// against the configured Postgres. Callers typically map to HTTP 503.
	ErrUpstreamUnreachable = errors.New("tenantpool: upstream unreachable")

	// ErrPoolExhausted is returned when the pool cannot acquire a
	// connection within its wait timeout. Callers typically map to
	// HTTP 503.
	ErrPoolExhausted = errors.New("tenantpool: pool exhausted")

	// ErrNoPool is the error value to report when PoolFromCtxOK returns
	// false — no pool was attached to the context, which means service
	// startup never called WithPool.
	ErrNoPool = errors.New("tenantpool: no pool in context")
)
