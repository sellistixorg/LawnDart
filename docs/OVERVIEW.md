# Overview

LawnDart is the .NET runtime that event-modeled systems compile into.

The durable log is recorded events (`IEventLog`). `IEventStore` is the typed
session over that log (`IEvent` in, `SequencedEvent` out). Third-party stores
implement the log, not the session.

The shapes an event model compiles into are on the
[CES matrix](CES_MATRIX.md). [Eventhesis](https://eventhesis.com) is a
separate, complementary product. It can output models (slice JSON) you
can use as specs for building with LawnDart. It does not generate LawnDart
code. See [Why LawnDart](WHY.md).

## Host grammar

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
var ctx = services.AddBoundedContext("default");
ctx.UseInMemory(); // or UseSqlServer(...)
ctx.WithCommandHandlers<SomeHandler>();
ctx.WithEventTypes<SomeEvent>();
ctx.WithProjections(...);
services.AddLawnDartHttpCommands(typeof(SomeCommand).Assembly);
app.MapLawnDartCommands();
```

## Packages (v1 happy path)

| Package | Role |
|---|---|
| `LawnDart` | Contracts: `ICommand`, `IEvent`, `IState`, aggregates, DCB, auth |
| `LawnDart.EventSourcing` | Runtime + InMemory store |
| `LawnDart.EventSourcing.SqlServer` | First durable store |
| `LawnDart.Messaging` | Reactors, processors, transports |
| `LawnDart.Messaging.InMemory` | In-process messaging |
| `LawnDart.Messaging.RabbitMQ` | RabbitMQ transport |
| `LawnDart.Messaging.AzureServiceBus` | Azure Service Bus transport |
| `LawnDart.Messaging.AwsSqs` | Amazon SQS transport. SNS fans each topic out to one queue per subscription. |
| `LawnDart.Messaging.SqlServer` | Durable inbox deduplication |
| `LawnDart.Messaging.Testing` | Transport conformance tests |
| `LawnDart.Projections.Lightweight` | Read-model host (in-memory stores) |
| `LawnDart.Projections.Lightweight.SqlServer` | SQL Server view and checkpoint stores |
| `LawnDart.Projections.Lightweight.Postgres` | Postgres view and checkpoint stores |
| `LawnDart.EventSourcing.Postgres` | Postgres event store and snapshots |
| `LawnDart.AspNetCore` | HTTP command mapping |
| `LawnDart.Authorization.AspNetCore` | HTTP claims to `AuthorizationContext` |
| `LawnDart.Testing` | Given / when / then harnesses |
| `LawnDart.Analyzers` | Optional `LDT*` schema-versioning diagnostics |

Academy with `UseInMemory()` needs no extra infrastructure. Per-package landing pages:
[Package map](packages/README.md). Transports:
[Message transports](MESSAGE_TRANSPORTS.md). Meters and activity sources:
[Telemetry](TELEMETRY.md).
