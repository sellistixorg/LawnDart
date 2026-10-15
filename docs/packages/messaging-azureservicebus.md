# LawnDart.Messaging.AzureServiceBus

Azure Service Bus `IMessageTransport`. Use it when a reactor or processor runs in
another process.

## Registration

```csharp
services.AddAzureServiceBusTransport(options =>
{
    options.ConnectionString = connectionString;
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

`AddAzureServiceBusTransport` calls `AddMessaging`. It registers the transport, the
topology setup, and an in-memory list of dead-lettered messages. Hosted
reactors and processors from `LawnDart.Messaging` subscribe through this
transport. Register an inbox before those hosts start. `AddSqlInboxStore`
keeps dedup across restarts.

Set `ConnectionString`, or set `FullyQualifiedNamespace` and `Credential`.
`Credential` is a `TokenCredential`. `DefaultAzureCredential` from Azure.Identity
is one. A `ServiceBusClient` already in the container is kept. Register a
`ServiceBusAdministrationClient` with it when the options do not carry a
connection string or a namespace, so topics and subscriptions can be created.

The emulator serves management on a different port than AMQP. Set
`AdministrationConnectionString` to that management connection string. When it
is unset, the data connection string or the namespace credential is used.

## Names

Publish creates the topic. The topic name comes from `ITopicMapper`. The
default mapper uses the `[EventTypeName]` token, so each type has its own
topic. A publish does not create a subscription.

The subscriber creates one subscription on that topic. The subscription name
is `SubscriptionName`. Types that map to the same topic share it. The
subscription rule lists `lawndart_type` for each type that subscription
handles, and the processor gives each message to the handler for that type.
`SetupTopology` true runs the same creation at startup for every event
type recorded by `AddReactor` and `AddEventProcessor`.

A topic is 1 to 260 characters. A subscription name is 1 to 50. Both use
letters, digits, periods, hyphens, and underscores, and start and end with
a letter or digit. A SQL filter is at most 1024 characters. Topic mapping
is in [Message transports](../MESSAGE_TRANSPORTS.md).

`Prefetch` is the processor prefetch count. `Concurrency` is
`MaxConcurrentCalls`. It is how many handlers may run at once.

`Configure<MessageTransportOptions>` is applied to `AzureServiceBusTransportOptions`.
A setting on the Service Bus configure delegate wins when both set the same field.

## Delivery

A handler that throws is tried again until `MaxDeliveryAttempts` (default 5).
Between tries the message is scheduled for `RetryDelay` (default 5 seconds),
then delivered to the same subscription. The retry sets `lawndart_target` to
that subscription name and keeps `lawndart_type`. A publish sets
`lawndart_target` to `*` and `lawndart_type` to the declared event type.
Each subscription's rule is
`(user.lawndart_target IS NULL OR user.lawndart_target = '*' OR user.lawndart_target = '<name>') AND (user.lawndart_type IS NULL OR user.lawndart_type = '<event-type>')`.
A message with no `lawndart_target` reaches every subscription that accepts
the event type. A message with no `lawndart_type` still matches. A retry
reaches only the subscription named in the property. With `DeadLetterFailedMessages`
left false, the attempt past that limit waits the same way and stays available.
Set the flag true to dead-letter it on the subscription. A warning is written,
and `messaging.deadlettered` increments.

`BrokerMaxDeliveryCount` is how many times the broker may deliver one locked
message before it dead-letters on its own. The default is 2000. Valid values
are 1 to 2000. The emulator accepts 1 to 10, so a test against the emulator
sets 10 or less. A scheduled retry completes the current delivery, so this
cap is not `MaxDeliveryAttempts`.

The retry is scheduled, then the original is completed. If the complete fails
after the schedule succeeds, both copies can be delivered.

A delivery that arrives with no handler waits `RetryDelay` too. It does not
spin on the subscription.

Several handlers on one subscription share that subscription. If one throws,
the message is delayed and every handler runs again.

The cancellation token passed to `SubscribeAsync` is the token the handler
receives. Cancelling it stops that subscription. The in-flight message is
scheduled again after `RetryDelay`. It is not abandoned with no delay.

`Dispose()` blocks the caller for up to `OperationTimeout` and then returns.
Cleanup can still be running. Prefer `DisposeAsync` (`await using`).

A publish the broker rejects is counted as a publish error. The duration
histogram records a publish after the broker accepts it. The caller sees the
exception, so an outbox row stays for another try.

User, tenant, and custom headers the publisher set are the values the
handler sees. They travel as application properties. Publish and consume
durations are recorded on the `LawnDart.Messaging` meter. Consume spans carry
`messaging.servicebus.topic`, `messaging.servicebus.subscription`,
`messaging.servicebus.delivery_count`, and `messaging.servicebus.redelivered`.

## Related

- [Package map](README.md)
- [Messaging](messaging.md)
- [Custom message transport](../CUSTOM_MESSAGE_TRANSPORT.md)
- [SQL Server inbox](messaging-sqlserver.md)
- [InMemory messaging](messaging-inmemory.md)
