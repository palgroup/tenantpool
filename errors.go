package tenantpool

import "errors"

// Sentinel errors returned by Registry operations and Middleware. Use
// errors.Is to detect them; custom error handlers should map them to
// HTTP responses as appropriate for the service.
var (
	// ErrInvalidConfig is returned at construction time or on Get with
	// bad input (empty tenantID, parse failure, ConfigurePool returning
	// an error). Treat as a programming or deployment error; 500.
	ErrInvalidConfig = errors.New("tenantpool: invalid config")

	// ErrTenantNotFound is returned when DSNBuilder fails for the given
	// tenant ID. Callers typically map to HTTP 404 so probes cannot
	// distinguish missing tenants from unreachable ones.
	ErrTenantNotFound = errors.New("tenantpool: tenant not found")

	// ErrUpstreamUnreachable is returned when the pooler or backend
	// Postgres cannot be reached. Callers typically map to HTTP 503.
	ErrUpstreamUnreachable = errors.New("tenantpool: upstream unreachable")

	// ErrPoolExhausted is returned when a tenant pool cannot acquire a
	// connection within its wait timeout. Callers typically map to
	// HTTP 503.
	ErrPoolExhausted = errors.New("tenantpool: pool exhausted")

	// ErrNoPool is returned by PoolFromCtxOK and ErrorHandler callbacks
	// when no pool was attached to the request context — a sign the
	// Middleware chain is misconfigured.
	ErrNoPool = errors.New("tenantpool: no pool in context")
)
