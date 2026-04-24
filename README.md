# tenantpool

Tenant-aware Postgres connection pool registry for Go HTTP services.

Single API for two deployment shapes:

- **Single-database** — one long-lived `*pgxpool.Pool` for every request.
- **Multi-tenant** — one pool per tenant ID, cached with LRU + idle
  eviction. The pooler behind the DSN (Supavisor, PgBouncer, direct PG)
  is a deployment detail; module code does not change when it swaps.

```go
import "github.com/palgroup/tenantpool"
```

## Quick start

### Single-database

```go
reg, err := tenantpool.New(tenantpool.Config{
    DatabaseURL: "postgres://user:pw@host:5432/db?sslmode=disable",
})
if err != nil { log.Fatal(err) }
defer reg.Close()

router.Use(reg.Middleware())

// In a handler:
pool := tenantpool.PoolFromCtx(r.Context())
```

### Multi-tenant (Supavisor)

```go
reg, err := tenantpool.New(tenantpool.Config{
    DSNBuilder: tenantpool.SupavisorDSN(tenantpool.SupavisorOpts{
        Host: "supavisor.internal:5432",
        UserPrefix: "auth",
        Password: pw,
    }),
    Resolver: tenantpool.HeaderResolver("X-Tenant-Ref"),
    MaxPoolsCached: 500,
    IdleTimeout:    15 * time.Minute,
})
if err != nil { log.Fatal(err) }
defer reg.Close()

// Background janitor (optional but recommended)
go func() {
    t := time.NewTicker(1 * time.Minute)
    defer t.Stop()
    for range t.C { reg.EvictIdle() }
}()

router.Use(reg.Middleware(
    tenantpool.WithErrorHandler(myEnvelopeWriter),
))

// Handlers are identical to the single-database case:
pool := tenantpool.PoolFromCtx(r.Context())
```

### Background workers

Workers with no HTTP request resolve the tenant from the message
payload, then build the ctx manually:

```go
pool, err := reg.Get(ctx, job.TenantID)
if err != nil { return err }
ctx = tenantpool.WithPool(ctx, pool)
return service.Process(ctx, job)
```

## Pooler swap

Changing from Supavisor to a direct-PG deployment is a one-line change
in `main.go`:

```go
DSNBuilder: tenantpool.DirectDSN(tenantpool.DirectOpts{
    Host: "postgres.internal:5432",
    User: "app", Password: pw,
}),
```

No handler, service, or repository code changes. `tenantpool.PoolFromCtx`
still returns a `*pgxpool.Pool`.

## Sentinel errors

Registry operations and the default error handler classify failures:

| Error                      | HTTP (default handler) | Meaning                                          |
|----------------------------|------------------------|--------------------------------------------------|
| `ErrTenantNotFound`        | 404                    | DSN builder could not resolve the tenant.        |
| `ErrUpstreamUnreachable`   | 503                    | Pooler or backend Postgres is unreachable.       |
| `ErrPoolExhausted`         | 503                    | Pool saturated under its wait timeout.           |
| `ErrInvalidConfig`         | 500                    | Config or programming error.                     |
| `ErrNoPool`                | —                      | PoolFromCtxOK returned (nil, false).             |

Plug a service-specific envelope with `tenantpool.WithErrorHandler`.

## Prometheus metrics

```go
metrics := tenantpool.NewMetrics(map[string]string{"module": "auth"})
reg.WithMetrics(metrics)
registerer.MustRegister(metrics.Collectors()...)
```

Exposed:

- `tenantpool_pools_active` — live pools.
- `tenantpool_pools_created_total` — new pools opened.
- `tenantpool_pools_evicted_total` — pools closed (LRU, idle, invalidate).
- `tenantpool_pool_errors_total{reason}` — failed Get calls by sentinel.
- `tenantpool_pool_acquire_duration_seconds` — pool acquire latency.

## Go version

Go 1.26+. Uses `pgx/v5` and `golang.org/x/sync/singleflight`.

## License

MIT.
