---
name: lawndart-event-store
description: Choose and register a LawnDart event store. UseInMemory for zero infra; UseSqlServer for durable SQL.
---

# Event store

v1 backends: **InMemory** and **SQL Server**. Both persist recorded frames
(`IEventLog`: `AppendEvent` in, `RecordedEvent` out). Application code uses
the typed session (`IEventStore`). InMemory serializes on append — it is
not an object heap. `IEventSerializer` is `ReadOnlyMemory<byte>` (UTF-8
JSON by default). Third-party stores implement the log, not the session.

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs).
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization.
3. **Load** by `string streamId` when the stream is not `{type}:{guid}`.
4. **Projections:** `ProjectionBase<TView>` plus attributes; multi-stream views implement `IMultiStreamEntityResolver`.
5. **Stores:** `UseInMemory()` / `UseSqlServer(...)` on `AddBoundedContext(name)`.

Excerpt from `samples/Library.Host/LibraryHost.cs` (`AddInMemoryLibrary`):

```csharp
var ctx = services.AddBoundedContext("default");
ctx.UseInMemory();
```

Excerpt from `samples/Library.Host/LibraryHost.cs` (`AddSqlLibrary`):

```csharp
ctx.UseSqlServer(o =>
{
    o.ConnectionString = connectionString;
    o.RequireTenantId = false;
});
```

Align `RequireTenantId` on `AddLawnDart` and `SqlServerEventStoreOptions`.

See `docs/BACKEND_SELECTION.md`.
