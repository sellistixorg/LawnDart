# Message transports

LawnDart ships three `IMessageTransport` packages. Pick one registration.
Reactors and processors live in `LawnDart.Messaging` and subscribe through
whichever transport you registered.

| Package | What it is | Registration |
|---|---|---|
| `LawnDart.Messaging.InMemory` | In-process transport. Delivery runs on the publish call. | `AddInMemoryMessaging()` |
| `LawnDart.Messaging.RabbitMQ` | RabbitMQ transport. Another process receives through the broker. | `AddRabbitMqTransport` |
| `LawnDart.Messaging.AzureServiceBus` | Azure Service Bus transport. Another process receives through the namespace. | `AddAzureServiceBusTransport` |

`LawnDart.Messaging.InMemory` is on nuget.org as a prerelease.
`LawnDart.Messaging.RabbitMQ` and `LawnDart.Messaging.AzureServiceBus`
are available starting with 0.6.0-alpha.1. Use `--prerelease` until a
stable version exists. Add the package for the transport you register.
Each transport package brings `LawnDart.Messaging`.

```bash
dotnet add package LawnDart.Messaging.InMemory --prerelease
dotnet add package LawnDart.Messaging.RabbitMQ --prerelease
dotnet add package LawnDart.Messaging.AzureServiceBus --prerelease
```

Pick one registration. These are alternatives. `AddRabbitMqTransport` and
`AddAzureServiceBusTransport` replace an `IMessageTransport` already
registered.

```csharp
services.AddInMemoryMessaging();
```

```csharp
services.AddRabbitMqTransport(options =>
{
    options.Uri = "amqp://rabbitmq:rabbitmq@localhost:5672/";
});
```

```csharp
services.AddAzureServiceBusTransport(options =>
{
    options.ConnectionString = connectionString;
});
```

The RabbitMQ URI is an AMQP (Advanced Message Queuing Protocol) URI. Set
`Uri`, or set `HostName`, `Port`, `UserName`, `Password`, and `VirtualHost`.
For Service Bus, set `ConnectionString`, or set `FullyQualifiedNamespace` and
`Credential`.

`MaxDeliveryAttempts` includes the first try. The default is 5.
`DeadLetterFailedMessages` defaults to false. Details per transport are below.
Package pages: [InMemory](packages/messaging-inmemory.md),
[RabbitMQ](packages/messaging-rabbitmq.md),
[Azure Service Bus](packages/messaging-azureservicebus.md).

## Retry delay

The in-process transport retries on the publish call. There is no wait
between tries.

RabbitMQ `RetryDelay` defaults to 5 seconds. A failed delivery waits on the
retry queue `{token}.{subscription}.retry`, then returns to the same
subscription queue. The wait is the time to live on that retry queue.

Azure Service Bus `RetryDelay` defaults to 5 seconds. A failed delivery is
scheduled for that delay, then delivered to the same subscription.

## Attempt counting

The first handler try is attempt 1.

The in-process transport counts from 1 up to `MaxDeliveryAttempts` inside
`MessageDelivery.DeliverAsync`.

RabbitMQ reads completed attempts from the header `x-lawndart-attempt`.
When that header is absent, it reads `x-delivery-count`. The current try is
that count plus 1. A retry writes `x-lawndart-attempt` with the number of
tries that have finished.

Azure Service Bus reads completed attempts from the application property
`lawndart-attempt`. A missing property counts as 0. The current try is that
count plus 1. A retry writes `lawndart-attempt` with the number of tries
that have finished.

`messaging.delivery.retries` on the `LawnDart.Messaging` meter counts a
failed attempt that will run again. The current span gets a
`messaging.retry` event tagged `messaging.attempt`. See
[Telemetry](TELEMETRY.md).

## Dead letter

`DeadLetterFailedMessages` defaults to false.

On the in-process transport, the last failure is thrown and the message is
not parked. Set the flag true to park the message on `IDeadLetterQueue` and
return. A caller such as the outbox still sees the failure when the flag is
false, so the outbox keeps the row. See [Outbox](OUTBOX_PATTERN.md).

On RabbitMQ, with the flag left false, the attempt past the limit waits
`RetryDelay` and stays on the queue. Set the flag true to nack it onto the
dead-letter exchange `{token}.dlx` and the queue `{token}.{subscription}.dlq`.

On Azure Service Bus, with the flag left false, the attempt past the limit
is scheduled again and stays available. Set the flag true to dead-letter it
on the subscription.

A parked message increments `messaging.deadlettered`. The current span gets
a `messaging.deadlettered` event tagged `messaging.attempt` and `error.type`,
and the span status is Error.

## Service Bus routing

Publish sets the application property `lawndart_target` to `*`. A retry sets
it to the subscription that failed. Each subscription's rule is:

```text
user.lawndart_target IS NULL OR user.lawndart_target = '*' OR user.lawndart_target = '<name>'
```

`<name>` is that subscription. A message with no `lawndart_target` reaches
every subscription. A retry reaches only the subscription named in the
property.

## Operation timeout

Azure Service Bus `OperationTimeout` is how long a Service Bus call and an
internal wait may run before they time out. The default is 30 seconds. The
value must be greater than zero.

## BrokerMaxDeliveryCount

`BrokerMaxDeliveryCount` is how many times Azure Service Bus may deliver one
locked message before the broker dead-letters it on its own. The default is
2000. Valid values are 1 to 2000. The emulator accepts 1 to 10, so a test
against the emulator sets 10 or less. A scheduled retry completes the
current delivery, so this cap is separate from `MaxDeliveryAttempts`.

## Unroutable publish

RabbitMQ publishes as mandatory. `UnroutablePublish` defaults to `Warn`.
When no queue is bound, the call logs a warning and returns. The broker did
not queue the message. Set `UnroutablePublish` to `Throw` when the caller
should see that failure. The outbox keeps the row when publish throws.
