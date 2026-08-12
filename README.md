# tenantpool

Postgres connection pool for a Palbase stack.

A stack serves one tenant and talks to one database, so this package holds
one `*pgxpool.Pool`: opened at boot from one DSN, carried on the context,
read by every handler, service and worker.

There is no per-request resolution. The pool is not chosen by a header, a
claim, or any other request-borne value — there is no second pool to choose
and no accessor that takes a tenant identifier. That is the phase-0 shape of
the isolation guarantee: the stack cannot be tricked into talking to another
tenant's database.

```go
import "github.com/palgroup/tenantpool"
```

## Quick start

```go
reg, err := tenantpool.New(tenantpool.Config{
    DatabaseURL: os.Getenv("DATABASE_URL"),
})
if err != nil { log.Fatal(err) }
defer reg.Close()

// Attach the pool once, at startup, to the root context of every entry
// point: the HTTP server, and each background worker that builds its own
// context (NATS consumers, Redis Streams consumers, cron jobs).
ctx = tenantpool.WithPool(ctx, reg.Pool())
```

In a handler:

```go
pool := tenantpool.PoolFromCtx(r.Context())
```

`PoolFromCtx` panics when no pool is attached — that means startup never
called `WithPool`, and failing silently would hide it. Use
`PoolFromCtxOK` where a missing pool is tolerable.

## Pool tuning

`Config` carries one field, so sizing and lifetime knobs travel inside the
DSN as pgxpool query parameters:

```
postgres://app:pw@postgres:5432/app?pool_max_conns=20&pool_min_conns=2&pool_max_conn_lifetime=1h
```

That keeps the whole connection target one string a deployment sets from one
environment variable.

## Sentinel errors

| Error                      | Meaning                                             |
|----------------------------|-----------------------------------------------------|
| `ErrInvalidConfig`         | `DatabaseURL` missing or unparseable — fails `New`. |
| `ErrUpstreamUnreachable`   | The pool could not be opened against Postgres.      |
| `ErrPoolExhausted`         | Pool saturated under its wait timeout.              |
| `ErrNoPool`                | `PoolFromCtxOK` returned `(nil, false)`.            |
| `ErrTenantNotFound`        | No producer left here; kept for consuming modules.  |

## Go version

Go 1.26+. Uses `pgx/v5`.

## License

MIT.
