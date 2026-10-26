# LawnDart.EventSourcing.Postgres

Postgres event store for LawnDart. `UsePostgres(...)` registers the same
keyed services as `UseSqlServer(...)`.

Postgres 15 or newer. The `default` context uses schema `lawndart`. Any
other context uses its own name. The `public` schema is rejected.

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default")
    .UsePostgres(o =>
    {
        o.ConnectionString = connectionString;
        o.RequireTenantId = false;
    });
```

Call `InitializeSchemaAsync` from a deploy or admin step before the first
append. Normal app startup calls `VerifySchemaAsync` instead. That call only
reads. `WithSnapshots` stores
snapshot state as bytes. `EnableOutbox = true` writes one outbox row per
event in that same transaction. See
[Schema initialization](https://github.com/sellistixorg/LawnDart/blob/main/docs/SCHEMA_INITIALIZATION.md).

See [the package page](https://github.com/sellistixorg/LawnDart/blob/main/docs/packages/eventsourcing-postgres.md).
Hosting, including PgBouncer, is in
[Postgres hosting](https://github.com/sellistixorg/LawnDart/blob/main/docs/POSTGRES_HOSTING.md).
