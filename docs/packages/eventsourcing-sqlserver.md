# LawnDart.EventSourcing.SqlServer

Durable SQL Server implementation of `IEventStore`. Same repositories and
domain types as InMemory; only the backend changes.

Take it when you leave the zero-infra path. Keep the same bounded-context
name so projections and HTTP mapping stay pointed at the same stack.

## Registration

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

## Related

- [Package map](README.md)
- [Backend selection](../BACKEND_SELECTION.md)
- [Outbox](../OUTBOX_PATTERN.md)
- [Production shape](../learning-path/08-production.md)
