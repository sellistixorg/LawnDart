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

Publish declares the exchange. The exchange and the routing key are the
`[EventTypeName]` token. A publish does not declare a queue.

The subscriber declares the queue `{token}.{subscription}`, the retry queue
`{token}.{subscription}.retry`, and the dead-letter queue
`{token}.{subscription}.dlq`. The dead-letter exchange is `{token}.dlx`.
`SetupTopology` true runs the same declarations at startup for every event
type recorded by `AddReactor` and `AddEventProcessor`.

Queues are quorum queues. Messages are persistent.

`Prefetch` is passed to BasicQos. `Concurrency` must be from 1 to 65535.
It is how many handlers may run at once.

`Configure<MessageTransportOptions>` is applied to `RabbitMqTransportOptions`.
A setting on the RabbitMQ configure delegate wins when both set the same field.

## Delivery

A handler that throws is tried again until `MaxDeliveryAttempts` (default 5).
Between tries the message sits on the retry queue for `RetryDelay`
(default 5 seconds), then returns to the same subscription queue.
With `DeadLetterFailedMessages` left false, the attempt past that limit
waits the same way and stays on the queue. Set the flag true to nack it
onto the dead-letter exchange. A warning is written, and
`messaging.deadlettered` increments.

A delivery that arrives with no handler waits on the retry queue too.
It does not spin on the subscription queue.

Several handlers on one subscription share that queue. If one throws, the
message is delayed and every handler runs again.

The cancellation token passed to `SubscribeAsync` is the token the handler
receives. Cancelling it stops that subscription. The message is nacked back
onto the queue.

A mandatory publish with no queue bound logs a warning and returns.
Set `UnroutablePublish` to `Throw` when the caller should see that failure.

User, tenant, and custom headers the publisher set are the values the
handler sees. Publish and consume durations are recorded on the
`LawnDart.Messaging` meter. Consume spans carry `messaging.rabbitmq.queue`
and `messaging.rabbitmq.redelivered`.

## Related

- [Package map](README.md)
- [Messaging](messaging.md)
- [Custom message transport](../CUSTOM_MESSAGE_TRANSPORT.md)
- [SQL Server inbox](messaging-sqlserver.md)
- [InMemory messaging](messaging-inmemory.md)
