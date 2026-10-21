# Package map

Seventeen packages. Take `LawnDart` + `LawnDart.EventSourcing` first; add the
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
| [`LawnDart.EventSourcing`](eventsourcing.md) | You need a runtime and an in-memory store. | `UseInMemory()` |
| [`LawnDart.EventSourcing.SqlServer`](eventsourcing-sqlserver.md) | You need a durable SQL Server store. | `UseSqlServer(...)` |
| [`LawnDart.EventSourcing.Postgres`](eventsourcing-postgres.md) | You need a durable Postgres store. | `UsePostgres(...)` |
| [`LawnDart.Projections.Lightweight`](projections-lightweight.md) | You need read models. | `WithProjections` / `MapProjectionQueries` |
| [`LawnDart.Projections.Lightweight.SqlServer`](projections-lightweight-sqlserver.md) | Read models must survive a restart on SQL Server. | `AddSqlProjectionStores` |
| [`LawnDart.Projections.Lightweight.Postgres`](projections-lightweight-postgres.md) | Read models must survive a restart on Postgres. | `AddPostgresProjectionStores` |
| [`LawnDart.Messaging`](messaging.md) | You need reactors or task processors. | `AddMessaging` / `AddReactor` |
| [`LawnDart.Messaging.InMemory`](messaging-inmemory.md) | You want choreography without a broker. | `AddInMemoryMessaging()` |
| [`LawnDart.Messaging.RabbitMQ`](messaging-rabbitmq.md) | You want a RabbitMQ broker. | `AddRabbitMqTransport` |
| [`LawnDart.Messaging.AzureServiceBus`](messaging-azureservicebus.md) | You want an Azure Service Bus namespace. | `AddAzureServiceBusTransport` |
| [`LawnDart.Messaging.AwsSqs`](messaging-awssqs.md) | You want Amazon SQS. SNS fans each topic out to one queue per subscription. | `AddAwsSqsTransport` |
| [`LawnDart.Messaging.SqlServer`](messaging-sqlserver.md) | Inbox dedup must survive a restart. | `AddSqlInboxStore` |
| [`LawnDart.Messaging.Testing`](messaging-testing.md) | You are writing a message transport and want the conformance suite. | Inherit `MessageTransportConformanceTests` |
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

Swap `.UseInMemory()` for `.UseSqlServer(...)` or `.UsePostgres(...)` and
`AddInMemoryProjectionStores` for `AddSqlProjectionStores` or
`AddPostgresProjectionStores` when you leave dev. The SQL Server methods are
in `LawnDart.Projections.Lightweight.SqlServer`. The Postgres methods are in
`LawnDart.Projections.Lightweight.Postgres`.
The bounded-context name stays the same. That swap is verified for
command dispatch, persist, reload, project, and read-back. Outbox and
subscriptions have their own tests. Inbox dedup swaps with `AddSqlInboxStore`
and `InitializeSqlInboxStoreAsync`. The transport you registered stays.
`AddInMemoryMessaging` is the in-process transport.
`AddRabbitMqTransport` is the RabbitMQ transport.
`AddAzureServiceBusTransport` is the Azure Service Bus transport.
`AddAwsSqsTransport` is the Amazon SQS transport. See
[Message transports](../MESSAGE_TRANSPORTS.md) and
[BACKEND_SELECTION.md](../BACKEND_SELECTION.md).

Log records store a payload-format number, `CodecId` (`byte`). The media
type stays on `IEventSerializer.ContentType`. SQL Server uses
one `EventData` (`VARBINARY`) column. `WithEventTypes` is required. Drop
and recreate SQL event, outbox, and inbox tables on a pre-1.0 schema change.
There is no in-place migration.

See the [extension method index](../EXTENSION_METHOD_INDEX.md).
