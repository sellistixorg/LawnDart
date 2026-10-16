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

Publish declares the exchange. The exchange is the topic from `ITopicMapper`.
The default mapper uses the `[EventTypeName]` token, so each type has its
own exchange. The routing key is always that token. A publish does not
declare a queue.

The subscriber declares one quorum queue and binds it to each event type it
handles. The routing key is the event type name. The queue name is the
subscription name. Set `QueueLayout` to `EventType` for one queue per event
type, named `{subscription}.{event-type}`.

One queue per subscription matches the usual RabbitMQ layout. The service
reads every type it handles from that queue. A slow type holds the others
behind it. Per-event queues give each type its own queue, so you can drain
or replay one type on its own. Retry and dead-letter stay one set for the
subscription in both layouts.

| Name | What it is |
|---|---|
| `{subscription}` | Quorum queue. Default layout. Bound once per handled event type. |
| `{subscription}.{event-type}` | Quorum queue when `QueueLayout` is `EventType`. |
| `{subscription}.retry` | Direct exchange and quorum queue. A failed delivery is published to the exchange with the event type as the routing key. The queue holds it for `RetryDelay`, then dead-letters it to the delay exchange with that routing key. |
| `{subscription}.delay` | Direct exchange. Routes the delayed message back to the subscription queue by event type. |
| `{subscription}.dlx` | Direct exchange. A dead letter is routed here by event type. |
| `{subscription}.dlq` | Quorum queue. Bound to the dead-letter exchange once per handled event type. |

Every name starts with the subscription name. Use a lowercase kebab-case
subscription name, such as `billing`. The event type is the
`[EventTypeName]` token. A dot separates the event type, when the layout
includes it, and the suffix. The suffixes are lowercase: `retry`, `delay`,
`dlx`, and `dlq`.

`{subscription}.retry` is both the exchange and the queue. RabbitMQ keeps
exchange names and queue names apart. The exchange is there so the event
type stays the routing key while the message waits.

These names differ from 0.6.0-alpha.1. That release used
`{event-type}.{subscription}`, `{event-type}.{subscription}.retry`,
`{event-type}.{subscription}.delay`, `{event-type}.dlx`, and
`{event-type}.{subscription}.dlq`. RabbitMQ will not change an existing
exchange's type. Drain the old queues, delete the old exchanges and queues,
or use a fresh vhost.

Azure Service Bus uses one subscription per service on a topic. The name is
the subscription name. A retry is a scheduled message on that subscription,
so there is no retry queue. A dead letter stays on the subscription's
built-in dead-letter sub-queue. Amazon SQS uses one queue per topic and
subscription, named `{subscription}--{topic}`, because a queue attaches to
one topic and a period is not a legal character. The dead-letter queue is
`{subscription}--{topic}--dlq`. A retry is a delayed send to the same queue.
The `dlq` word matches RabbitMQ. The name starts with the subscription.
The topic keeps two topics for one service distinct, and the name has to
fit in 80 characters.

`SetupTopology` true runs the same declarations at startup for every event
type recorded by `AddReactor` and `AddEventProcessor`. When several types
share an exchange, each queue receives only the types bound to it.

An exchange, routing key, or queue name is at most 255 bytes. Names that
start with `amq.` are reserved. Leave room for `.retry`, the longest suffix.
Topic mapping and grouping are in [Message transports](../MESSAGE_TRANSPORTS.md).

Queues are quorum queues. Messages are persistent.

`Prefetch` is passed to BasicQos. `Concurrency` must be from 1 to 65535.
It is how many handlers may run at once.

`Configure<MessageTransportOptions>` is applied to `RabbitMqTransportOptions`.
A setting on the RabbitMQ configure delegate wins when both set the same field.

## Delivery

A handler that throws is tried again until `MaxDeliveryAttempts` (default 5).
Between tries the message sits on `{subscription}.retry` for `RetryDelay`
(default 5 seconds), then returns through `{subscription}.delay` to the
same subscription queue. With `DeadLetterFailedMessages` left false, the
attempt past that limit waits the same way and stays on the queue. Set the
flag true to nack it onto `{subscription}.dlx`. It lands on
`{subscription}.dlq`. A warning is written, and `messaging.deadlettered`
increments.

A delivery that arrives with no handler waits on the retry queue too.
It does not spin on the subscription queue.

Several handlers for one event type share that queue. If one throws, the
message is delayed and every handler for that type runs again. A handler
for another type on the same queue is not called for that message.

The cancellation token passed to `SubscribeAsync` is the token the handler
receives. Cancelling it stops that subscription. The in-flight message waits
on the retry queue for `RetryDelay`. The attempt count does not increase.

A mandatory publish with no queue bound logs a warning and returns. That
publish is not a successful `messaging.publish.duration` measurement. It
increments `messaging.publish.errors` with `messaging.publish.outcome` set
to `unroutable`. Set `UnroutablePublish` to `Throw` when the caller should
see that failure.

`OperationTimeout` is how long to wait for automatic connection recovery
before opening a new connection. The default is 30 seconds. The value must
be greater than zero.

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
