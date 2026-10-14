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

Publish creates the topic. The topic name is the `[EventTypeName]` token.
A publish does not create a subscription.

The subscriber creates one subscription on that topic. The subscription name
is `SubscriptionName`. `SetupTopology` true runs the same creation at startup
for every event type recorded by `AddReactor` and `AddEventProcessor`.

`Prefetch` is the processor prefetch count. `Concurrency` is
`MaxConcurrentCalls`. It is how many handlers may run at once.

`Configure<MessageTransportOptions>` is applied to `AzureServiceBusTransportOptions`.
A setting on the Service Bus configure delegate wins when both set the same field.

## Delivery

A handler that throws is tried again until `MaxDeliveryAttempts` (default 5).
Between tries the message is scheduled for `RetryDelay` (default 5 seconds),
then delivered to the same subscription. With `DeadLetterFailedMessages` left
false, the attempt past that limit waits the same way and stays available.
Set the flag true to dead-letter it on the subscription. A warning is written,
and `messaging.deadlettered` increments.

The subscription's broker delivery cap is 2000 where the namespace allows it.
The Service Bus emulator allows 10, and this transport uses 10 there.
A scheduled retry completes the current delivery, so that cap is not the
attempt limit. The attempt limit is `MaxDeliveryAttempts`.

A delivery that arrives with no handler waits `RetryDelay` too. It does not
spin on the subscription.

Several handlers on one subscription share that subscription. If one throws,
the message is delayed and every handler runs again.

The cancellation token passed to `SubscribeAsync` is the token the handler
receives. Cancelling it stops that subscription. The in-flight message is
scheduled again after `RetryDelay`. It is not abandoned with no delay.

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
