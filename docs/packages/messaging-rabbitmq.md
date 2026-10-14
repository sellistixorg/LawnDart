# LawnDart.Messaging.RabbitMQ

RabbitMQ `IMessageTransport`. Use it when a reactor or processor runs in
another process.

## Registration

```csharp
services.AddRabbitMqTransport(options =>
{
    options.Uri = "amqp://rabbitmq:rabbitmq@localhost:5672/";
    options.SubscriptionName = "billing";
    options.Prefetch = 20;
    options.Concurrency = 4;
    options.MaxDeliveryAttempts = 5;
    options.DeadLetterFailedMessages = true;
    options.SetupTopology = true;
});
services.AddSqlInboxStore(connectionString);
services.AddReactor<PaymentReactor, SeatReservationConfirmed>();
```

`AddRabbitMqTransport` calls `AddMessaging`. It registers the transport, the
topology setup, and an in-memory list of dead-lettered messages. Hosted
reactors and processors from `LawnDart.Messaging` subscribe through this
transport. Register an inbox before those hosts start. `AddSqlInboxStore`
keeps dedup across restarts.

Set `Uri`, or set `HostName`, `Port`, `UserName`, `Password`, and
`VirtualHost`.

## Names

The exchange and the routing key are the `[EventTypeName]` token.
The queue is `{token}.{subscription}`. The dead-letter exchange is
`{token}.dlx`. Its routing key and the dead-letter queue are
`{token}.{subscription}` and `{token}.{subscription}.dlq`.
Queues are quorum queues. Messages are persistent. The broker delivery
limit is off, so a message stays on the queue when dead-letter handling
is off. Turn dead-letter handling on to park it.

`Prefetch` is passed to BasicQos. `Concurrency` is how many handlers may
run at once.

## Delivery

A handler that throws is tried again until `MaxDeliveryAttempts` (default 5).
The broker redelivers the same message. With `DeadLetterFailedMessages` left
false, the attempt past that limit is nacked and the message stays on the
queue. Set the flag true to nack it onto the dead-letter exchange. A warning
is written, and `messaging.deadlettered` increments.

The cancellation token passed to `SubscribeAsync` is the token the handler
receives. Cancelling it stops that subscription. The message is nacked back
onto the queue.

User, tenant, and custom headers the publisher set are the values the
handler sees. Publish and consume durations are recorded on the
`LawnDart.Messaging` meter. Consume spans carry `messaging.rabbitmq.queue`
and `messaging.rabbitmq.redelivered`.

`SetupTopology` true runs `RabbitMqTopologySetup` at startup for every event
type recorded by `AddReactor` and `AddEventProcessor`. Publish and subscribe
declare the same exchanges, queues, and bindings, so a message waits on the
queue when the consumer is not running yet.

## Related

- [Package map](README.md)
- [Messaging](messaging.md)
- [Custom message transport](../CUSTOM_MESSAGE_TRANSPORT.md)
- [SQL Server inbox](messaging-sqlserver.md)
- [InMemory messaging](messaging-inmemory.md)
