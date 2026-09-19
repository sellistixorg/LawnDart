# Package map

Ten packages. Take `LawnDart` + `LawnDart.EventSourcing` first; add the
others when you need a durable store, read models, HTTP, tests, or
compile-time schema checks.

The `IEventStore` interface lives in core `LawnDart`. It is the typed
session (`IEvent` in, `SequencedEvent` out). Backends implement `IEventLog`
(`AppendEvent` in, `RecordedEvent` out): `InMemoryEventStore` in
`LawnDart.EventSourcing`, `SqlServerEventStore` in
`LawnDart.EventSourcing.SqlServer`. Application code depends on the session
without taking a backend. InMemory serializes on append. It is not an
object heap. `IEventSerializer` reads and writes `ReadOnlyMemory<byte>`
(UTF-8 JSON by default).

Host grammar: [DI Grammar](../DI_GRAMMAR.md). Envelope fields:
[Metadata](../METADATA.md). Shapes: [CES matrix](../CES_MATRIX.md).

| Package | Take it when | Registration |
|---|---|---|
| [`LawnDart`](core.md) | Always. Contracts and host core. | `AddLawnDart` / `AddBoundedContext` |
| [`LawnDart.EventSourcing`](eventsourcing.md) | You need a runtime and a zero-infra store. | `UseInMemory()` |
| [`LawnDart.EventSourcing.SqlServer`](eventsourcing-sqlserver.md) | You need a durable store. | `UseSqlServer(...)` |
| [`LawnDart.Projections.Lightweight`](projections-lightweight.md) | You need read models. | `WithProjections` / `MapProjectionQueries` |
| [`LawnDart.Messaging`](messaging.md) | You need reactors or task processors. | `AddMessaging` / `AddReactor` |
| [`LawnDart.Messaging.InMemory`](messaging-inmemory.md) | You want choreography without a broker. | `AddInMemoryMessaging()` |
| [`LawnDart.AspNetCore`](aspnetcore.md) | You want HTTP POST to a command. | `AddLawnDartHttpCommands` / `MapLawnDartCommands` |
| [`LawnDart.Authorization.AspNetCore`](authorization-aspnetcore.md) | You want HTTP claims on those commands. | `AddHttpAuthorizationContext()` |
| [`LawnDart.Testing`](testing.md) | You want given / when / then against InMemory. | `BddTestContext.CreateInMemory(...)` |
| [`LawnDart.Analyzers`](analyzers.md) | You want `LDT*` schema-versioning diagnostics at `dotnet build`. | Package reference (optional) |

Happy-path host:

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddInMemoryProjectionStores("default");
services.AddBoundedContext("default")
    .UseInMemory()
    .WithCommandHandlers<MyHandler>()
    .WithEventTypes<MyEvent>()
    .WithProjections([typeof(MyProjection).Assembly]);

services.AddInMemoryMessaging();
services.AddLawnDartHttpCommands(typeof(MyCommand).Assembly);
app.MapLawnDartCommands();
app.MapProjectionQueries("default");
```

Swap `.UseInMemory()` for `.UseSqlServer(...)` and
`AddInMemoryProjectionStores` for `AddSqlProjectionStores` when you leave
dev. The bounded-context name stays the same. That swap is verified for
command dispatch, persist, reload, project, and read-back. Outbox and
subscriptions have their own tests. See
[BACKEND_SELECTION.md](../BACKEND_SELECTION.md).

Log frames store a `CodecId` (`byte`), not a MIME string. SQL Server uses
one `EventData` (`VARBINARY`) column. `WithEventTypes` is required. Drop
and recreate SQL event and outbox tables on a pre-1.0 schema change.
There is no in-place migration.

See the [extension method index](../EXTENSION_METHOD_INDEX.md).
