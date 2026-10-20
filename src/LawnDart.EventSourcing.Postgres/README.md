# LawnDart.EventSourcing.Postgres

Postgres event store for LawnDart. `UsePostgres(...)` registers the same
keyed services as `UseSqlServer(...)`.

Postgres 15 or newer. Tables live in a schema you name. The default is
`lawndart`. The `public` schema is rejected.

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default")
    .UsePostgres(o =>
    {
        o.ConnectionString = connectionString;
        o.RequireTenantId = false;
    });
```

Call `InitializeSchemaAsync` before the first append. `EnableOutbox = true`
is rejected. Leave it false until the outbox release.

See [the package page](../../docs/packages/eventsourcing-postgres.md).
