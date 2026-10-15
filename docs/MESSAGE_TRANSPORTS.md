# Message transports

LawnDart ships four `IMessageTransport` packages. Pick one registration.
Reactors and processors live in `LawnDart.Messaging` and subscribe through
whichever transport you registered.

| Package | What it is | Registration |
|---|---|---|
| `LawnDart.Messaging.InMemory` | In-process transport. Delivery runs on the publish call. | `AddInMemoryMessaging()` |
| `LawnDart.Messaging.RabbitMQ` | RabbitMQ transport. Another process receives through the broker. | `AddRabbitMqTransport` |
| `LawnDart.Messaging.AzureServiceBus` | Azure Service Bus transport. Another process receives through the namespace. | `AddAzureServiceBusTransport` |
| `LawnDart.Messaging.AwsSqs` | Amazon SQS transport. SNS fans each topic out to one queue per subscription. | `AddAwsSqsTransport` |

`LawnDart.Messaging.InMemory` is on nuget.org as a prerelease.
`LawnDart.Messaging.RabbitMQ`, `LawnDart.Messaging.AzureServiceBus`, and
`LawnDart.Messaging.AwsSqs` first ship in the upcoming 0.6.0-alpha.1.
Use `--prerelease` until a stable version exists. Add the package for the
transport you register. Each transport package brings `LawnDart.Messaging`.

```bash
dotnet add package LawnDart.Messaging.InMemory --prerelease
dotnet add package LawnDart.Messaging.RabbitMQ --prerelease
dotnet add package LawnDart.Messaging.AzureServiceBus --prerelease
dotnet add package LawnDart.Messaging.AwsSqs --prerelease
```

Pick one registration. These are alternatives. `AddRabbitMqTransport`,
`AddAzureServiceBusTransport`, and `AddAwsSqsTransport` replace an
`IMessageTransport` already registered.

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

```csharp
services.AddAwsSqsTransport(options =>
{
    options.Region = "us-east-1";
});
```

The RabbitMQ URI is an AMQP (Advanced Message Queuing Protocol) URI. Set
`Uri`, or set `HostName`, `Port`, `UserName`, `Password`, and `VirtualHost`.
For Service Bus, set `ConnectionString`, or set `FullyQualifiedNamespace` and
`Credential`. For SQS, set `Region`, or set `ServiceUrl` for LocalStack.
Credentials come from the AWS SDK default chain. An `IAmazonSQS` and
`IAmazonSimpleNotificationService` already registered are kept.

`MaxDeliveryAttempts` includes the first try. The default is 5.
`DeadLetterFailedMessages` defaults to false. Details per transport are below.
Package pages: [InMemory](packages/messaging-inmemory.md),
[RabbitMQ](packages/messaging-rabbitmq.md),
[Azure Service Bus](packages/messaging-azureservicebus.md),
[Amazon SQS](packages/messaging-awssqs.md).

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

Amazon SQS `RetryDelay` defaults to 5 seconds. A failed delivery is sent
to that subscription's own queue after the delay. It is not published to
the topic. A cancelled delivery uses that same wait. The attempt count does
not increase. SQS delay is whole seconds and at most 15 minutes. A delay
under one second is sent as 1 second.

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

Amazon SQS reads completed attempts from the message attribute
`lawndart-attempt`. A missing attribute counts as 0. The current try is that
count plus 1. A retry writes `lawndart-attempt` with the number of tries
that have finished. When the message already has 10 attributes, the count
is stored inside `lawndart-headers` instead. A process crash does not write
that attribute. The message returns after `VisibilityTimeout`, and SQS
counts another receive. While a handler runs, the transport extends
visibility at about half of `VisibilityTimeout` (default 30 seconds), up to
240 extensions. A failed extension is logged. The handler can then be
delivered again if it is still running.

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

On Amazon SQS, with the flag left false, the attempt past the limit is sent
again to the same queue and stays available. LawnDart does not set an SQS
redrive policy, so SQS does not move the message. A crash returns it after
`VisibilityTimeout` with the same attempt count. Set the flag true to send
the last failure to `{topic}--{subscription}--dlq`. The queue then also has
a redrive policy. `MaxReceiveCount` defaults to 10 (1 to 1000). SQS moves
the message after that many receives of the same copy. A delayed retry is a
new message, so this count is the backstop for a receive that was never
deleted. A cancelled delivery still does not increment `lawndart-attempt`.

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

## SQS routing

Publish sets the message attribute `lawndart_type` to the declared event
type name (`AwsSqsNames.EventTypeAttribute`). User, tenant, custom headers,
and the rest of the envelope travel in one JSON attribute,
`lawndart-headers`. SNS and SQS allow 10 message attributes.

Each subscription is one SQS queue on the SNS topic, with raw message
delivery. The body is the message JSON. When several types share a topic,
the queue's filter is:

```text
{"lawndart_type":[{"exists":false},"<event-type>",...]}
```

`<event-type>` is each declared type that queue handles. A matching
`lawndart_type` reaches only those queues. A message that omits
`lawndart_type` and carries some other attribute matches `exists: false`
and reaches every queue on the topic. A queue that handles one type accepts
it as that type. SNS does not deliver a message that has no attributes at
all once a filter is set. A retry is sent to the failing queue with
`DelaySeconds`. It is not published to the topic.

## Topic mapping

`ITopicMapper` maps a declared `[EventTypeName]` to the topic or exchange
that carries it. The default is `DeclaredTypeTopicMapper`: the topic is the
declared name. One type, one topic. RabbitMQ, Azure Service Bus, and
Amazon SQS read `MessageTransportOptions.TopicMapper`. The in-process
transport does not.

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

`SanitizingTopicMapper` replaces each period with a hyphen. The default
mapper stays the declared name. Use the sanitizing mapper for SNS, which
rejects periods:

```csharp
options.TopicMapper = new SanitizingTopicMapper();
```

`order.placed` becomes `order-placed`. Pass an inner mapper when grouping
should happen first. Host startup calls `EnsureNoCollisions` for the
recorded types. `order.placed` and `order-placed` throw, because both
sanitize to `order-placed`. Two names that the inner mapper already sends
to one topic do not.

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

Amazon SQS publishes to the mapped topic. Each subscription queue subscribes
to that topic. The filter is the `lawndart_type` policy above. When one
queue handles several types, the policy lists each of them. The consumer
dispatches the body to the handler for that type. A retry is a delayed send
to that queue. The other queue does not receive it.

A message that omits `lawndart_type` still matches the type clause. On a
shared topic, set the property so each subscription receives only its types.
Publish through the transport sets it.

### Startup and rule changes

`AddReactor` and `AddEventProcessor` record the event types for the host.
The first subscribe on a topic writes one rule that lists every recorded
type for that topic, including a type whose handler has not subscribed yet.
A later subscribe can add a type that was not recorded. The default does
not remove a type from the rule, including a type left by an earlier deploy.

A message for a recorded type with no handler waits `RetryDelay` and is
delivered again. The application property `lawndart-deferrals` counts those
waits. Each wait increments `messaging.deferred`. From the third wait, a
warning is written. After `MaxHandlerDeferrals` (default 12) the wait counts
as a failed attempt, so `MaxDeliveryAttempts` and `DeadLetterFailedMessages`
apply. See [Telemetry](TELEMETRY.md).

To drop a type an earlier deploy left on the rule, set `ReplaceRouteRule`
to true and start again. The rule then lists only the types this process
recorded. Deleting the subscription does the same on the next start: the
process creates it from the recorded types. The new rule is created before
the old one is deleted. After that swap, a publish of a removed type does
not enter the subscription.

A rule that only grows can pass 1024 characters. Publish and subscribe then
fail and name that limit. Shorten the event type names, split the topic, or
set `ReplaceRouteRule` so the filter lists only the current types.

A filter change creates the new rule, then deletes the previous one. The
new rule is named `lawndart-route`, or `lawndart-route-2` and upward when
that name is already in use. Two hosts that pick the same name both finish:
the second reads the rules again, and a rule that already has the same
filter counts as success. The same swap upgrades a subscription whose rule
only tests `lawndart_target`. A publish during that overlap still matches a
rule, so the message is not dropped.

SQS installs the filter when the queue is subscribed. The policy only grows.
A later subscribe adds a type and leaves types from an earlier deploy in
place. `exists: false` stays, so a change does not start dropping messages
that omit `lawndart_type`. Set `ReplaceRouteRule` to true to list only the
types this process recorded. After a write, the transport waits a short
jittered delay and reads the policy again, three times. A stale write from
another host is merged back in. A write that lands after that last read is
not repaired until the next subscribe. Two hosts that add different types
can still drop one type in that window. On AWS, a change to an existing
filter can take time to apply. The first policy is set on the subscribe
call. A subscribe that loses to "already exists with different attributes"
reads the subscription again and merges.

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

An SNS topic is 1 to 256 characters. An SQS queue is 1 to 80. Letters,
digits, hyphens, and underscores. A period is not allowed. A declared name
such as `order.placed` needs a topic mapper. The queue name is
`{topic}--{subscription}`. The dead-letter queue appends `--dlq` and must
also fit in 80 characters. A name that ends with `.fifo` is rejected.
FIFO topics and queues are not supported.

An SNS filter policy is at most 256 KB and at most 150 values. The missing
`lawndart_type` match counts as one value, so one queue can list 149 event
types. A longer list fails and names that limit. Shorten the names or split
the topic.

SNS and SQS allow 10 message attributes. LawnDart uses `lawndart_type` and
`lawndart-headers` on a publish, and adds `lawndart-attempt` and
`lawndart-deferrals` when a delivery is delayed. User, tenant, and custom
headers are inside `lawndart-headers`.

An SQS or SNS message is at most 256 KiB, including attributes. Publish and
retry check that size first and throw `AwsSqsMessageTooLargeException`.
A retry that cannot be sent is dead-lettered when that handling is on.
When it is off, the message is deleted and logged. It is not sent again.

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

Amazon SQS `OperationTimeout` is how long an AWS call and an internal wait
may run before they time out. The default is 30 seconds. The value must be
greater than zero. A receive waits the shorter of this and `LongPollWait`
(default 20 seconds, maximum 20). `Dispose()` blocks the caller for up to
`OperationTimeout` and then returns. Cleanup can still be running. Prefer
`DisposeAsync` (`await using`).

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

Amazon SQS checks for an SQS subscription before it publishes. When none is
subscribed, `UnroutablePublish` defaults to `Warn`. The call logs a warning
and returns. The message was not sent. That publish is not recorded on
`messaging.publish.duration`. It increments `messaging.publish.errors` with
`messaging.publish.outcome` set to `unroutable`. Set `UnroutablePublish` to
`Throw` when the caller should see that failure. The outbox keeps the row
when publish throws. A thrown publish is a `messaging.publish.errors` count
without the unroutable outcome tag. SNS would accept a publish that has no
subscriber and then drop it. The check avoids counting that as success.
