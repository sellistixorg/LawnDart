---
name: lawndart-bounded-context
description: Register named LawnDart bounded contexts, keyed stores, and WithCommandHandlers. Use when one process hosts more than one event store.
---

# Bounded context

Each context calls `UseInMemory()` / `UseSqlServer(...)` and
`WithEventTypes`. A store-backed context without a catalog fails at warmup.
Register handlers with `WithCommandHandlers<TMarker>()` on that context.

Excerpt from `samples/Library.Host/LibraryHost.cs` (`AddTwoContexts`):

```csharp
var library = services.AddBoundedContext("library");
library.UseInMemory();
library.WithCommandHandlers<BorrowBookHandler>();
library.WithEventTypes<BookAdded>();

var other = services.AddBoundedContext("other");
other.UseInMemory();
other.WithEventTypes<BookAdded>();
```

Single-store apps use name `"default"`. Keyed `IEventStore` /
`IAggregateRepository` / `IDcbRepository` resolve with that name.
The `"default"` context also gets unkeyed aliases (store, repositories, and
handlers) so HTTP can resolve them without a hand-written bridge. Named
contexts stay keyed-only.

Do not treat contexts as tenants. Tenant IDs belong on stream IDs / metadata.
