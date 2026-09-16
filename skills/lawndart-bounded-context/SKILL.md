---
name: lawndart-bounded-context
description: Register named LawnDart bounded contexts, keyed stores, and WithCommandHandlers. Use when one process hosts more than one event store.
---

# Bounded context

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs) — `WithCommandHandlers<TMarker>()` per context.
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization.
3. **Load** by `string streamId` when the stream is not `{type}:{guid}`.
4. **Projections:** `ProjectionBase<TView>` plus attributes; multi-stream views implement `IMultiStreamEntityResolver`.
5. **Stores:** each context calls `UseInMemory()` / `UseSqlServer(...)`.

Excerpt from `samples/Library.Host/LibraryHost.cs` (`AddTwoContexts`):

```csharp
var library = services.AddBoundedContext("library");
library.UseInMemory();
library.WithCommandHandlers<BorrowBookHandler>();
library.WithEventTypes<BookAdded>();

var other = services.AddBoundedContext("other");
other.UseInMemory();
```

Single-store apps use name `"default"`. Keyed `IEventStore` /
`IAggregateRepository` / `IDcbRepository` resolve with that name.
The `"default"` context also gets unkeyed aliases (store, repositories, and
handlers) so HTTP can resolve them without a hand-written bridge. Named
contexts stay keyed-only.

Do not treat contexts as tenants. Tenant IDs belong on stream IDs / metadata.
