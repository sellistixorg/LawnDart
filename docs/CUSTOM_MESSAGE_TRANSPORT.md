# Custom message transport

A transport implements `IMessageTransport` and uses the shared pieces in
`LawnDart.Messaging`. The host calls your transport. Your transport calls the
shared helpers for the type name, JSON, headers, and delivery.

`IMessageTransport` has two methods: `PublishAsync` and `SubscribeAsync`.
`MessageContext` carries the ids and the header bag. Those two types are the
transport boundary.

## Register

```csharp
services.AddMessaging();
services.AddSingleton<IMessageTransport, MyTransport>();
services.Configure<MessageTransportOptions>(options =>
{
    options.SubscriptionName = "billing";
    options.Concurrency = 4;
    options.Prefetch = 20;
    options.MaxDeliveryAttempts = 5;
    options.DeadLetterFailedMessages = false;
});
```

`AddMessaging` registers `MessageTransportStartupCheck`. The host runs that
check while it is starting, before reactors and processors take traffic.

## Declared type name

Call `MessageTypeNames.Require(typeof(TMessage))` when you publish or subscribe.
It returns the `[EventTypeName]` token. A type without that attribute throws
`InvalidOperationException`. The message type is that token.

`AddReactor` and `AddEventProcessor` record the event type. The startup check
calls `Require` for each recorded type. The host fails to start when a name
is missing. A publish of some other undeclared type fails at that publish.

## JSON

Serialize and deserialize message bodies with `MessagingJson.Options`.
Property names are PascalCase, matching the event store. Matching ignores
case. Null properties are omitted. The outbox publisher uses the same object.
The instance is read-only.

Use this object for every message body so a transport reads the same bytes
the outbox and the event store write.

## Headers

Write the envelope with `MessageEnvelopeHeaders.Write`. Read it with
`MessageEnvelopeHeaders.Read`. Names live on `MessageHeaders`.

| Constant | Wire name |
|---|---|
| `MessageId` | `message-id` |
| `MessageType` | `message-type` |
| `CorrelationId` | `correlation-id` |
| `CausationId` | `causation-id` |
| `UserId` | `user-id` |
| `TenantId` | `tenant-id` |
| `EnqueuedAt` | `enqueued-at` |
| `CustomPrefix` | `header.` |
| `OutboxStreamId` | `outbox.streamId` |
| `OutboxSequencePosition` | `outbox.sequencePosition` |
| `OutboxEventType` | `outbox.eventType` |
| `TraceParent` | `traceparent` |
| `TraceState` | `tracestate` |

Application headers are stored with the `header.` prefix and restored without
it. `traceparent`, `tracestate`, and the `outbox.*` headers are copied under
their own names. User, tenant, and custom headers the publisher set are the
values the handler sees.

`enqueued-at` is a round-trip timestamp (`o` format). A value that does not
parse fails the read.

## Delivery

Register a hook with `AddMessagePreHandler<THandler>()`. The hook's
`BeforeHandleAsync` runs before the message handler, on every attempt, in
registration order. A failure in the hook counts as a failed attempt.

An in-process transport calls `MessageDelivery.DeliverAsync`. That method
retries until `MaxDeliveryAttempts`, including the first try. The default
leaves `DeadLetterFailedMessages` false, so the last failure is thrown and the
caller still has the message. Set the flag true to park the message on
`IDeadLetterQueue` and return. A cancelled token stops delivery and does not
dead-letter the message.

A broker that redelivers on its own calls `MessageDelivery.TryOnceAsync` once
per delivery, then dead-letters when the broker's count reaches
`MaxDeliveryAttempts`. Calling `DeliverAsync` and also letting the broker
redeliver multiplies the attempts.

`MemoryDeadLetterQueue` is an in-process queue. A broker transport can park
on its own dead-letter destination and still expose those messages through
`IDeadLetterQueue` so the conformance suite can read them.

## Topology

Implement `IMessageTopologySetup` when the transport can create topics, queues,
or exchanges. Leave `SetupTopology` false until you want that step. The
startup check skips setup when the flag is false. When the flag is true and
no `IMessageTopologySetup` is registered, the host fails to start.

`MessageSubscription` lists the event types from `AddReactor` and
`AddEventProcessor`. Create one destination per declared type name and bind
`SubscriptionName`.

`InMemoryTopologySetup` records `{token}/{subscription}` and does not open a
broker.

## Settings

`MessageTransportOptions.EnsureValid` rejects a blank subscription name,
concurrency below 1, prefetch below 0, and max attempts below 1. The startup
check calls it. A subclass that adds connection settings should call
`base.EnsureValid()` and then check its own fields. Register the common
settings on `MessageTransportOptions` so the host check sees them.

InMemory checks concurrency and prefetch and still delivers on the publish
call. A broker transport uses those values when it fetches and dispatches.

## Prove it

Reference `LawnDart.Messaging.Testing` from the transport's test project.
Inherit `MessageTransportConformanceTests` and implement `CreateFixture`.
Return the transport, the options it is using, and the dead-lettered messages.

The suite publishes `ConformancePing` (`messaging.conformance-ping`). It
checks:

- a publish reaches the subscriber
- user, tenant, and custom headers arrive unchanged, along with the other envelope fields
- the pre-handler hook runs before the handler
- a handler that fails and then succeeds is retried within the max attempts and is not dead-lettered
- a handler that keeps failing is dead-lettered after the max attempts
- cancelling the subscription stops later delivery
- a type without `[EventTypeName]` is rejected

`InMemoryTransportConformanceTests` is the in-process run. Broker tests that
need a container use `Category=Integration`.
