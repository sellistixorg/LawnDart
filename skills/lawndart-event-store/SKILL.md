---
name: lawndart-event-store
description: Choose and register a LawnDart event store. UseInMemory for zero infra; UseSqlServer for durable SQL.
---

# Event store

v1 backends: **InMemory** and **SQL Server**.

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs).
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization.
3. **Load** by `string streamId` when the stream is not `{type}:{guid}`.
4. **Projections:** `ProjectionBase<TView>` plus attributes; multi-stream views implement `IMultiStreamEntityResolver`.
5. **Stores:** `UseInMemory()` / `UseSqlServer(...)` on `AddBoundedContext(name)`.

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);

// Local / tests / Academy
services.AddBoundedContext("default").UseInMemory();

// Durable
services.AddBoundedContext("default")
    .UseSqlServer(o =>
    {
        o.ConnectionString = cs;
        o.RequireTenantId = false;
    });
```

Align `RequireTenantId` on `AddLawnDart` and `SqlServerEventStoreOptions`.

See `docs/BACKEND_SELECTION.md`.
