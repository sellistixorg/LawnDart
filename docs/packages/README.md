# Package map

Nine packages. Take `LawnDart` + `LawnDart.EventSourcing` first; add the
others when you need a durable store, read models, HTTP, or tests.

The **`IEventStore` interface** lives in core `LawnDart`. Implementations
live elsewhere: `InMemoryEventStore` in `LawnDart.EventSourcing`,
`SqlServerEventStore` in `LawnDart.EventSourcing.SqlServer`. Depend on the
abstraction without taking a backend.

| Package | Take it when | Registration |
|---|---|---|
| [`LawnDart`](core.md) | Always. Contracts and host core. | `AddLawnDart` / `AddBoundedContext` |
| [`LawnDart.EventSourcing`](eventsourcing.md) | You need a runtime and a zero-infra store. | `UseInMemory()` |
| [`LawnDart.EventSourcing.SqlServer`](eventsourcing-sqlserver.md) | You need a durable store. | `UseSqlServer(...)` |
| [`LawnDart.Projections.Lightweight`](projections-lightweight.md) | You need read models. | `WithProjections` / `MapProjectionQueries` |
| [`LawnDart.Messaging`](messaging.md) | You need reactors or task processors. | `AddMessaging` / `AddReactor` |
| [`LawnDart.Messaging.InMemory`](messaging-inmemory.md) | You want choreography without a broker. | `AddInMemoryMessaging()` |
| [`LawnDart.AspNetCore`](aspnetcore.md) | You want HTTP POST → command. | `AddLawnDartHttpCommands` / `MapLawnDartCommands` |
| [`LawnDart.Authorization.AspNetCore`](authorization-aspnetcore.md) | You want HTTP claims on those commands. | `AddHttpAuthorizationContext()` |
| [`LawnDart.Testing`](testing.md) | You want given / when / then against InMemory. | `BddTestContext.CreateInMemory()` |

Happy-path host:

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddInMemoryProjectionStores("default");
services.AddBoundedContext("default")
    .UseInMemory()
    .WithProjections([typeof(MyProjection).Assembly]);

services.AddInMemoryMessaging();
services.AddLawnDartHttpCommands(typeof(MyCommand).Assembly);
app.MapLawnDartCommands();
app.MapProjectionQueries("default");
```

Swap `.UseInMemory()` for `.UseSqlServer(...)` and
`AddInMemoryProjectionStores` for `AddSqlProjectionStores` when you leave
dev. The bounded-context name stays the same.

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs).
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization.
3. **Load** by `string streamId` when the stream is not `{type}:{guid}`.
4. **Projections:** `ProjectionBase<TView>` plus attributes; multi-stream views implement `IMultiStreamEntityResolver`.
5. **Stores:** `AddBoundedContext(name).UseInMemory()` / `UseSqlServer(...)`.

See [DI Grammar](../DI_GRAMMAR.md) and the
[extension method index](../EXTENSION_METHOD_INDEX.md).
