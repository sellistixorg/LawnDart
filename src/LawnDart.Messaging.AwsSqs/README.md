# LawnDart.Messaging.AwsSqs

Amazon SQS transport for `LawnDart.Messaging`. SNS fans each topic out to one queue per subscription.

## What this project provides

- `AddAwsSqsTransport(...)` registration
- Topics named from `ITopicMapper`. The default mapper uses the declared message type. Publish creates the topic. Each subscriber creates one queue and subscribes it to that topic with raw message delivery.
- A retry delay (default 5 seconds) that returns only to the queue that failed. With dead-letter handling on, the last failure is sent to that subscription's dead-letter queue.

## Routing

Publish sets the message attribute `lawndart_type` to the declared event type.
The other envelope headers, including user, tenant, and custom headers, travel in one JSON attribute, `lawndart-headers`. SNS and SQS allow 10 message attributes.
A retry is sent to that subscription's queue. It is not published to the topic.

When several types share a topic, the subscription filter is:

```text
{"lawndart_type":[{"exists":false},"<event-type>",...]}
```

A message whose `lawndart_type` is one of those types is delivered to that queue.
A message that omits `lawndart_type` and carries some other attribute is delivered too.
SNS does not deliver a message that has no attributes at all once a filter is set.
A queue that handles one type accepts that message as its type.

FIFO topics and queues are not supported.

`SanitizingTopicMapper` replaces a period in a topic name with a hyphen.
The default mapper is unchanged. `VisibilityTimeout` defaults to 30 seconds
and is extended while a handler runs. With dead-letter handling on, the
queue has an SQS redrive policy (`MaxReceiveCount`, default 10). With it
off, LawnDart does not set that policy.

Pair with `LawnDart.Messaging` for reactors and processors.
Pair with `LawnDart.Messaging.SqlServer` when inbox dedup must survive a restart.
