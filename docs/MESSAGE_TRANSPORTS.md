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
A cancelled delivery uses that same wait. The attempt count does not increase.

Azure Service Bus `RetryDelay` defaults to 5 seconds. A failed delivery is
scheduled for that delay, then delivered to the same subscription.
A cancelled delivery uses that same wait.

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
it to the subscription that failed. Publish also sets `lawndart_type` to the
declared event type name (`AzureServiceBusNames.EventTypeProperty`).
Each subscription's rule is:

```text
(user.lawndart_target IS NULL OR user.lawndart_target = '*' OR user.lawndart_target = '<name>') AND (user.lawndart_type IS NULL OR user.lawndart_type = '<event-type>')
```

`<name>` is that subscription. `<event-type>` is each declared type that
subscription handles. A message with no `lawndart_target` reaches every
subscription that accepts the event type. A message with no `lawndart_type`
still matches. A retry reaches only the subscription named in
`lawndart_target`, and only when that subscription handles the event type.

## Topic mapping

`ITopicMapper` maps a declared `[EventTypeName]` to the topic or exchange
that carries it. The default is `DeclaredTypeTopicMapper`: the topic is the
declared name. One type, one topic. RabbitMQ and Azure Service Bus read
`MessageTransportOptions.TopicMapper`. The in-process transport does not.

Set the mapper on the transport options, or register `ITopicMapper`. A mapper
set on the options wins. A registered mapper is used when the options still
have the default instance.

Group types that share a bounded context:

```csharp
options.TopicMapper = new GroupTopicMapper(new Dictionary<string, string>
{
    ["order-placed"] = "orders",
    ["order-shipped"] = "orders",
    ["invoice-issued"] = "billing",
});
```

A type that is not listed keeps its declared name. Pass a fallback topic
when every other type should share one name.

Dotted names can use the prefix before the first separator:

```csharp
options.TopicMapper = new PrefixTopicMapper('.');
```

`orders.placed` maps to `orders`. A name with no separator maps to itself.

Send every type to one topic:

```csharp
options.TopicMapper = new SharedTopicMapper("lawndart");
```

`DelegateTopicMapper` calls a function you supply. `TopicFor` must return a
non-empty name. The same declared type should keep the same topic for the
life of the process.

### Filtering

RabbitMQ publishes to the mapped exchange. The routing key is the declared
event type name. Each subscription queue binds to that exchange with the
routing key of the type it handles. A publish of one type reaches only the
queues bound for that type. The queue name stays
`{event-type}.{subscription}`. A retry is published to that subscription's
retry queue and returns to the same queue. It does not cross to another
subscription or another event type. Dead-letter queues stay per event type
and subscription (`{event-type}.{subscription}.dlq`), even when the publish
exchange is shared.

Azure Service Bus publishes to the mapped topic. Subscriptions on that topic
share it. The SQL rule is the `lawndart_target` clause and the
`lawndart_type` clause above. When one subscription handles several types,
the type clause lists each of them with `OR`. The processor dispatches the
body to the handler for that type. A retry sets `lawndart_target` to the
subscription that failed and keeps `lawndart_type`, so the other
subscription does not receive it.

A message that omits `lawndart_type` still matches the type clause. On a
shared topic, set the property so each subscription receives only its types.
Publish through the transport sets it.

### Startup and rule changes

`AddReactor` and `AddEventProcessor` record the event types for the host.
The first subscribe on a topic writes one rule that lists every recorded
type for that topic, including a type whose handler has not subscribed yet.
A later subscribe can add a type that was not recorded. It does not remove
a type from the rule.

A filter change creates the new rule, then deletes the previous one. The
new rule is named `lawndart-route`, or `lawndart-route-2` and upward when
that name is already in use. The same swap upgrades a subscription whose
rule only tests `lawndart_target`. A publish during that overlap still
matches a rule, so the message is not dropped.

### Moving a RabbitMQ type

Binding a type to a grouped exchange adds that binding. The binding on the
previous exchange stays. Delete the old binding when that exchange should
stop delivering the type.

### Naming limits

A Service Bus topic is 1 to 260 characters. Letters, digits, periods,
hyphens, and underscores. It must start and end with a letter or digit.
The mapped topic has to pass those rules. A subscription name is 1 to 50
characters, with the same character rules.

A Service Bus SQL filter is at most 1024 characters. A subscription that
lists many long event type names can exceed that. Shorten the names or
split the topic.

A RabbitMQ exchange, routing key, or queue name is at most 255 bytes.
Names that start with `amq.` are reserved. The queue name is
`{event-type}.{subscription}`. The retry queue adds `.retry`. The
dead-letter queue adds `.dlq`. Leave room for the longest suffix.

A single quote in a subscription name or event type name is doubled in the
SQL filter.

## Operation timeout

Azure Service Bus `OperationTimeout` is how long a Service Bus call and an
internal wait may run before they time out. The default is 30 seconds. The
value must be greater than zero. `Dispose()` blocks the caller for up to
that long and then returns. Cleanup can still be running. Prefer
`DisposeAsync` (`await using`).

RabbitMQ `OperationTimeout` is how long to wait for automatic connection
recovery before opening a new connection. The default is 30 seconds. The
value must be greater than zero. When the wait ends first, the transport
opens a new connection.

## BrokerMaxDeliveryCount

`BrokerMaxDeliveryCount` is how many times Azure Service Bus may deliver one
locked message before the broker dead-letters it on its own. The default is
2000. Valid values are 1 to 2000. The emulator accepts 1 to 10, so a test
against the emulator sets 10 or less. A scheduled retry completes the
current delivery, so this cap is separate from `MaxDeliveryAttempts`.

## Unroutable publish

RabbitMQ publishes as mandatory. `UnroutablePublish` defaults to `Warn`.
When no queue is bound, the call logs a warning and returns. The broker did
not queue the message. That publish is not recorded on
`messaging.publish.duration`. It increments `messaging.publish.errors` with
`messaging.publish.outcome` set to `unroutable`. The publish span gets that
tag and status Error. Set `UnroutablePublish` to `Throw` when the caller
should see that failure. The outbox keeps the row when publish throws. A
thrown publish is a `messaging.publish.errors` count without the unroutable
outcome tag.
