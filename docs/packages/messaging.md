# LawnDart.Messaging

Reactors, event processors, task processors, and outbox publisher hooks.
`ICommandDispatcher` lives in Core (`LawnDart.Messaging` namespace); this
package type-forwards it and calls it from hosted reactors/processors.

Take it when an event in one slice should become a command (or further work)
in another. This package is the host and the contracts; it does not include
a transport. Pair it with `LawnDart.Messaging.InMemory` for zero-broker
delivery. Pair it with `LawnDart.Messaging.RabbitMQ` for a RabbitMQ broker.
Pair it with `LawnDart.Messaging.AzureServiceBus` for Azure Service Bus.

## Registration

```csharp
services.AddMessaging();
services.AddReactor<PaymentReactor, SeatReservationConfirmed>();
services.AddTaskProcessor<OverdueRegistrationProcessor>();
```

`AddInMemoryMessaging()`, `AddRabbitMqTransport`, and
`AddAzureServiceBusTransport` each call `AddMessaging()` for you.

| Method | Pattern |
|---|---|
| `AddReactor<TReactor, TEvent>` | Event → Command |
| `AddEventProcessor<TProcessor, TEvent>` | Event → Event / side work |
| `AddTaskProcessor<TProcessor>` | State → Command |

## Transport pieces

Reactors and processors require `[EventTypeName]` on the event. Host startup
fails when the attribute is missing. The message type is that token.

`MessagingJson.Options` is the JSON setting for message bodies and the outbox
publisher. Property names are PascalCase. Reads ignore case. Null values are
omitted.

`MessageHeaders` names the envelope headers. `MessageTransportOptions` sets
the subscription name, concurrency, prefetch, max delivery attempts, and
dead-letter handling. `TopicMapper` maps a declared event type to a topic
or exchange. The default is one topic per type.
`AddMessagePreHandler<THandler>()` runs a hook before each handler attempt.

Set `MessageTransportOptions.SetupTopology` to true to create topics, queues,
or exchanges at startup. The host skips that step when the flag is false.

Write a transport with [Custom message transport](../CUSTOM_MESSAGE_TRANSPORT.md).
Prove it with [`LawnDart.Messaging.Testing`](messaging-testing.md).

## Meter

The `LawnDart.Messaging` meter carries:

| Instrument | When |
|---|---|
| `messaging.publish.duration` | A publish finishes |
| `messaging.consume.duration` | One subscriber finishes a delivery |
| `messaging.publish.errors` | A publish throws, or a RabbitMQ publish in Warn mode reached no queue (`messaging.publish.outcome` = `unroutable`) |
| `messaging.inbox.duplicates` | The inbox drops a duplicate |
| `messaging.delivery.retries` | A failed attempt will be tried again |
| `messaging.deferred` | A delivery waited because its handler was not subscribed yet |
| `messaging.deadlettered` | A message is parked |

Retry and dead-letter measurements carry `messaging.message_type` and
`messaging.subscription_name`. Publish and consume spans use the same activity
source name. Every LawnDart meter and activity source is on
[Telemetry](../TELEMETRY.md). Transport settings are on
[Message transports](../MESSAGE_TRANSPORTS.md).

## Related

- [Package map](README.md)
- [EDA patterns](../EDA-Patterns.md)
- [Reactions](../learning-path/06-reactions.md)
- [Custom message transport](../CUSTOM_MESSAGE_TRANSPORT.md)
- [InMemory messaging](messaging-inmemory.md)
- [RabbitMQ](messaging-rabbitmq.md)
- [Azure Service Bus](messaging-azureservicebus.md)
- [SQL Server inbox](messaging-sqlserver.md)
