# LawnDart.EventSourcing.SqlServer

SQL Server event store. Pair with `AddBoundedContext(name).UseSqlServer(...)`.

```csharp
ctx.UseSqlServer(o =>
{
    o.ConnectionString = connectionString;
    o.RequireTenantId = false;
});
```

Optional `WithSnapshots` and outbox (`EnableOutbox`). Integration tests use
Testcontainers and are tagged `Category=Integration`.

InMemory remains the documented default for local work and Academy.
