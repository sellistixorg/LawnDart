# LawnDart.Messaging.AwsSqs

Amazon SQS `IMessageTransport`. SNS fans each topic out to one queue per subscription.
Use it when a reactor or processor runs in another process.

First ships in the upcoming 0.6.0-alpha.1.

## Registration

```csharp
services.AddAwsSqsTransport(options =>
{
    options.Region = "us-east-1";
    options.SubscriptionName = "billing";
    options.Prefetch = 10;
    options.Concurrency = 4;
    options.MaxDeliveryAttempts = 5;
    options.DeadLetterFailedMessages = true;
    options.SetupTopology = true;
});
services.AddSqlInboxStore(connectionString);
services.AddReactor<PaymentReactor, SeatReservationConfirmed>();
```

`AddAwsSqsTransport` calls `AddMessaging`. It registers the transport, the
topology setup, and an in-memory list of dead-lettered messages. Hosted
reactors and processors from `LawnDart.Messaging` subscribe through this
transport. Register an inbox before those hosts start. `AddSqlInboxStore`
keeps dedup across restarts.

Set `Region`, or set `ServiceUrl` for LocalStack (`http://localhost:4566`).
Credentials come from the AWS SDK default chain. An `IAmazonSQS` and
`IAmazonSimpleNotificationService` already in the container are kept.
Register both when the options do not carry a region or a service URL.

LocalStack still needs credentials the default chain can read. A test key
pair in the environment is enough. `ServiceUrl` selects the endpoint.
When `Region` is empty, the signing region is `us-east-1`.

## Names

Publish creates the SNS topic. The topic name comes from `ITopicMapper`.
The default mapper uses the `[EventTypeName]` token, so each type has its
own topic. A publish does not create a queue.

The subscriber creates one SQS queue and subscribes it to that topic with
raw message delivery. The queue name is `{topic}--{subscription}`.
The dead-letter queue is `{topic}--{subscription}--dlq`.
Types that map to the same topic share it. The filter lists `lawndart_type`
for each type that subscription handles, and the consumer gives each message
to the handler for that type. `SetupTopology` true runs the same creation
at startup for every event type recorded by `AddReactor` and
`AddEventProcessor`.

An SNS topic is 1 to 256 characters. An SQS queue is 1 to 80. Both use
letters, digits, hyphens, and underscores. A period is not allowed, so a
token such as `order.placed` needs a topic mapper.
`SanitizingTopicMapper` replaces each period with a hyphen. The default
mapper is unchanged. Startup rejects two declared names that sanitize to
one topic unless the inner mapper already grouped them. The dead-letter
suffix `--dlq` has to fit in the 80 characters as well. FIFO names
(`.fifo`) are rejected. FIFO is not supported. Topic mapping is in
[Message transports](../MESSAGE_TRANSPORTS.md).

`Concurrency` is how many receive loops run. `Prefetch` is how many messages
one receive asks for, capped at 10, and capped by the free loops.
`LongPollWait` is the receive wait, from 0 to 20 seconds. The default is
20. The receive uses the shorter of that and `OperationTimeout`.
`VisibilityTimeout` is how long a received message stays hidden. The
default is 30 seconds. While the handler runs, visibility is extended at
about half that interval, up to 240 times. A failed extension is logged.

`Configure<MessageTransportOptions>` is applied to `AwsSqsTransportOptions`.
A setting on the SQS configure delegate wins when both set the same field.

## Delivery

A handler that throws is tried again until `MaxDeliveryAttempts` (default 5).
Between tries the message is sent to the same queue with `RetryDelay`
(default 5 seconds). SQS delay is whole seconds, at most 15 minutes. A delay
under one second is sent as 1 second. The retry sets `lawndart-attempt` to
the number of tries that have finished. It does not publish to the topic, so
another subscription does not receive it.

Publish sets `lawndart_type` to the declared event type. User, tenant, custom
headers, and the rest of the envelope travel in `lawndart-headers`. SNS and
SQS allow 10 message attributes. A message that already has 10, and already
has `lawndart-headers`, keeps the attempt count inside that JSON. A message
that already has 10 and has no `lawndart-headers` cannot store another count.

When several types share a topic, the filter is
`{"lawndart_type":[{"exists":false},"<event-type>",...]}`.
A message with a matching `lawndart_type` reaches only the queues that list
that type. A message that omits `lawndart_type` and carries some other
attribute matches `exists: false` and reaches every queue on the topic.
A queue that handles one type accepts it as that type. A queue that handles
several types fails that delivery, because it cannot pick a handler.
SNS does not deliver a message that has no attributes at all once a filter
is set. `exists: false` matches only when some other attribute is present.

The filter only grows. A later subscribe adds a type. It does not remove one.
Set `ReplaceRouteRule` to true to list only the types this process recorded.
The `exists: false` match stays, so that change does not start dropping
messages that omit `lawndart_type`. After a write, the transport waits a
short jittered delay and reads the policy again, three times. A stale write
from another host is merged back in. A write that lands after that last
read is not repaired until the next subscribe. An SNS filter allows 150
values. The missing-attribute match counts as one, so 149 event types fit.

On AWS, a filter change can take time to apply. Creating the subscription
with the filter already set avoids that wait for the first policy.

With `DeadLetterFailedMessages` left false, the attempt past the limit waits
the same way and stays on the queue. LawnDart does not set an SQS redrive
policy, so a crash returns the message after `VisibilityTimeout` and the
attempt count is unchanged. Set the flag true to send the last failure to
`{topic}--{subscription}--dlq`. The queue then has a redrive policy.
`MaxReceiveCount` defaults to 10. SQS moves a message that was received that
many times without being deleted. A warning is written, and
`messaging.deadlettered` increments. A cancel does not increment
`lawndart-attempt`. A delayed retry is a new SQS message, so the redrive
count starts over.

The copy is sent, then the original is deleted. If the delete fails after
the send succeeds, both copies can be delivered.

A delivery that arrives with no handler waits `RetryDelay` too. It does not
spin on the queue. `lawndart-deferrals` counts those waits. After
`MaxHandlerDeferrals` (default 12) the wait counts as a failed attempt.

Several handlers on one queue share that queue. If one throws, the message
is delayed and every handler runs again.

The cancellation token passed to `SubscribeAsync` is the token the handler
receives. Cancelling it stops that subscription. The in-flight message is
sent again after `RetryDelay`. The attempt count does not increase.
User and tenant headers are copied onto `MessageContext`. They are not an
authorization context.

`Dispose()` blocks the caller for up to `OperationTimeout` and then returns.
Cleanup can still be running. Prefer `DisposeAsync` (`await using`).

Publish and retry check the 256 KiB limit, including attributes, before the
send. Over that limit the call throws `AwsSqsMessageTooLargeException`.
A retry that cannot be sent is dead-lettered when that handling is on.
When it is off, the message is deleted and logged. It is not sent again.

A publish the service rejects is counted as a publish error. The duration
histogram records a publish after SNS accepts it. When no queue is subscribed,
the default `UnroutablePublish` is `Warn`: the call logs a warning and returns,
and the message was not sent. That publish is not recorded on
`messaging.publish.duration`. It increments `messaging.publish.errors` with
`messaging.publish.outcome` set to `unroutable`. Set `UnroutablePublish` to
`Throw` when the caller should see that failure. The caller sees the
exception, so an outbox row stays for another try.

Publish and consume durations are recorded on the `LawnDart.Messaging` meter.
Consume spans carry `messaging.sns.topic`, `messaging.sqs.queue`,
`messaging.sqs.approximate_receive_count`, and `messaging.sqs.redelivered`.

## Related

- [Package map](README.md)
- [Messaging](messaging.md)
- [Custom message transport](../CUSTOM_MESSAGE_TRANSPORT.md)
- [SQL Server inbox](messaging-sqlserver.md)
- [InMemory messaging](messaging-inmemory.md)
