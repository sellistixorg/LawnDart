# LawnDart.EventSourcing

Runtime plus the in-memory **implementation** of `IEventStore`.

The `IEventStore` **interface** is in core `LawnDart`. This package provides
`InMemoryEventStore`, `AggregateRepository`, `DcbRepository`, JSON
serialization, and outbox processing.

Take it for local work, tests, and Academy. Swap the backend later without
changing domain types.

## Registration

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default").UseInMemory();
```

Registers keyed `IEventStore`, `IAggregateRepository`, and `IDcbRepository`
for that context name. The `"default"` context also gets unkeyed aliases, so
`GetRequiredService<IAggregateRepository>()` works without a key.

`WithCommandHandlers` scans handler assemblies.

## Related

- [Package map](README.md)
- [Quickstart](../QUICKSTART.md)
- [Backend selection](../BACKEND_SELECTION.md)
- [SqlServer package](eventsourcing-sqlserver.md)
