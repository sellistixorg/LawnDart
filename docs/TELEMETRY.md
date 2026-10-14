# Telemetry

LawnDart records metrics on a `Meter` and traces on an `ActivitySource`.
Pass the name below to OpenTelemetry `AddMeter` and `AddSource`.

`LawnDart.Snapshots` is a meter constructed with version `1.0.0`.
`LawnDart.Projections` is a meter and an activity source, both constructed
with version `1.0.0`. The other meters and sources are constructed with the
name only.

Names are taken from `new Meter` and `new ActivitySource` under `src/`.

## Setup

`AddOpenTelemetry` comes from the `OpenTelemetry.Extensions.Hosting` package.

```csharp
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter(
        "LawnDart.Snapshots",
        "LawnDart.Messaging",
        "LawnDart.EventProcessor",
        "LawnDart.Reactor",
        "LawnDart.TaskProcessor",
        "LawnDart.Outbox",
        "LawnDart.EventStore",
        "LawnDart.Dcb",
        "LawnDart.Projections",
        "LawnDart.Authorization"))
    .WithTracing(tracing => tracing.AddSource(
        "LawnDart.Messaging",
        "LawnDart.EventProcessor",
        "LawnDart.Reactor",
        "LawnDart.TaskProcessor",
        "LawnDart.EventStore",
        "LawnDart.Dcb",
        "LawnDart.Projections",
        "LawnDart.Authorization",
        "LawnDart.CommandDispatcher",
        "LawnDart.AspNetCore"));
```

`AddLightweightProjectionMetrics` and `AddLightweightProjectionTracing`
(`LawnDart.Projections.Lightweight`) add `LawnDart.Projections` only.

## Meters

A unit of `none` means the instrument was created without a unit.

### LawnDart.Snapshots

Package: `LawnDart`. Tags on every instrument: `entity_type` (short type
name) and `kind` (`aggregate` or `dcb`).

| Name | Type | Unit | Tags |
|---|---|---|---|
| `snapshot.hits_total` | Counter of long | none | `entity_type`, `kind` |
| `snapshot.misses_total` | Counter of long | none | `entity_type`, `kind` |
| `snapshot.writes_total` | Counter of long | none | `entity_type`, `kind` |
| `snapshot.write_failures_total` | Counter of long | none | `entity_type`, `kind` |
| `snapshot.write_drops_total` | Counter of long | none | `entity_type`, `kind` |
| `snapshot.delta_events` | Histogram of long | `events` | `entity_type`, `kind` |
| `snapshot.restore_duration_ms` | Histogram of double | `ms` | `entity_type`, `kind` |
| `snapshot.write_duration_ms` | Histogram of double | `ms` | `entity_type`, `kind` |

`snapshot.restore_duration_ms` is recorded on a hit and on a miss.
`snapshot.write_duration_ms` is recorded on a completed write.
`snapshot.delta_events` is recorded on a hit.

### LawnDart.Messaging

Package: `LawnDart.Messaging`.

| Name | Type | Unit | Tags |
|---|---|---|---|
| `messaging.publish.duration` | Histogram of double | `ms` | `messaging.message_type` |
| `messaging.consume.duration` | Histogram of double | `ms` | `messaging.message_type` |
| `messaging.publish.errors` | Counter of long | `count` | `messaging.message_type` |
| `messaging.inbox.duplicates` | Counter of long | `count` | `messaging.message_type` |
| `messaging.delivery.retries` | Counter of long | `count` | `messaging.message_type`, `messaging.subscription_name` |
| `messaging.deadlettered` | Counter of long | `count` | `messaging.message_type`, `messaging.subscription_name` |

### LawnDart.EventProcessor

Package: `LawnDart.Messaging`.

| Name | Type | Unit | Tags |
|---|---|---|---|
| `event_processor.process.duration` | Histogram of double | `ms` | `event_processor.type`, `event_processor.event_type` |
| `event_processor.events.emitted` | Counter of long | `count` | `event_processor.type` |
| `event_processor.errors` | Counter of long | `count` | `event_processor.type`, `event_processor.event_type` |

`event_processor.type` is the processor's short type name.
`event_processor.event_type` is the declared event type name.
`event_processor.events.emitted` is recorded when the processor returns at
least one event.

### LawnDart.Reactor

Package: `LawnDart.Messaging`.

| Name | Type | Unit | Tags |
|---|---|---|---|
| `reactor.handle.duration` | Histogram of double | `ms` | `reactor.type`, `reactor.event_type` |
| `reactor.commands.emitted` | Counter of long | `count` | `reactor.type` |
| `reactor.errors` | Counter of long | `count` | `reactor.type`, `reactor.event_type` |

`reactor.type` is the reactor's short type name.
`reactor.event_type` is the declared event type name.
`reactor.commands.emitted` is recorded when the reactor returns at least one
command.

### LawnDart.TaskProcessor

Package: `LawnDart.Messaging`.

| Name | Type | Unit | Tags |
|---|---|---|---|
| `task_processor.poll.duration` | Histogram of double | `ms` | `task_processor.type` |
| `task_processor.commands.emitted` | Counter of long | `count` | `task_processor.type` |
| `task_processor.errors` | Counter of long | `count` | `task_processor.type` |

`task_processor.type` is the processor's short type name.
`task_processor.commands.emitted` is recorded when the poll returns at least
one command.

### LawnDart.Outbox

Package: `LawnDart.EventSourcing`.

| Name | Type | Unit | Tags |
|---|---|---|---|
| `outbox.publish.failures` | Counter of long | `count` | `outbox.event_type` |
| `outbox.deadlettered` | Counter of long | `count` | `outbox.event_type` |

`outbox.event_type` is the family token stored on the outbox row.

### LawnDart.EventStore

Package: `LawnDart.EventSourcing`.

| Name | Type | Unit | Tags |
|---|---|---|---|
| `lawndart_events_appended_total` | Counter of long | `events` | `stream_id` |
| `lawndart_events_read_total` | Counter of long | `events` | `operation`, `stream_id` |
| `lawndart_append_duration_seconds` | Histogram of double | `seconds` | `stream_id` |
| `lawndart_read_duration_seconds` | Histogram of double | `seconds` | `operation`, `stream_id` |
| `lawndart_append_errors_total` | Counter of long | none | `stream_id` |

`lawndart_append_errors_total` is created when an append is recorded as
unsuccessful. It has no description in code. A read with no stream id tags
`stream_id` as `unknown`.

### LawnDart.Dcb

Package: `LawnDart`. DCB is Dynamic Consistency Boundary.

| Name | Type | Unit | Tags |
|---|---|---|---|
| `dcb.repository.get_state.duration` | Histogram of double | `ms` | none |
| `dcb.repository.get_state.event_count` | Histogram of long | `count` | none |
| `dcb.repository.append.duration` | Histogram of double | `ms` | none |
| `dcb.entity.emit.count` | Counter of long | `count` | none |
| `dcb.repository.handle_command.duration` | Histogram of double | `ms` | `command.type` |

`command.type` is the command's short type name.

### LawnDart.Projections

Package: `LawnDart.Projections.Lightweight`.

Some instruments record only when a `ProjectionTelemetryOptions` flag is on.
`EnableExtendedMetrics`, `EnableCacheMetrics`, and `EnableFlushMetrics`
default to true. `EnableHighDetailTags` defaults to false.

| Name | Type | Unit | Tags |
|---|---|---|---|
| `projections.events.processed` | Counter of long | none | `projection.type`, `event.type` |
| `projections.events.failed` | Counter of long | none | `projection.type`, `event.type`, `error.type` |
| `projections.checkpoints.saved` | Counter of long | none | `projection.type` |
| `projections.flush.errors` | Counter of long | none | `projection.type` |
| `projections.read.total` | Counter of long | none | `projection.type`, and `source` when high-detail tags are on |
| `projections.read.cache.hit` | Counter of long | none | `projection.type` |
| `projections.read.cache.miss` | Counter of long | none | `projection.type` |
| `projections.read.stale_rejected` | Counter of long | none | `projection.type` |
| `projections.workingset.evictions` | Counter of long | none | `projection.type`, and `reason` when high-detail tags are on |
| `projections.event.processing.duration` | Histogram of double | `ms` | `projection.type` |
| `projections.poll.duration` | Histogram of double | `ms` | `projection.type` |
| `projections.flush.duration` | Histogram of double | `ms` | `projection.type` |
| `projections.flush.views` | Histogram of int | none | `projection.type` |
| `projections.checkpoint.lag` | Histogram of long | none | `projection.type` |
| `projections.lag.pipeline` | Histogram of double | `ms` | `projection.type` |
| `projections.lag.pipeline.projection` | Histogram of double | `ms` | `projection.type` |
| `projections.instance.mailboxsize` | Histogram of int | none | `instance.id` |
| `projections.coordinator.mailboxsize` | Histogram of int | none | `id` |
| `projections.supervisor.mailboxsize` | Histogram of int | none | `id` |
| `projections.statsaggregator.mailboxsize` | Histogram of int | none | `id` |
| `projections.instances.active` | Observable gauge of long | none | none |
| `projections.lag.total` | Observable gauge of long | `ms` | none |
| `projections.lag.pipeline.max` | Observable gauge of long | `ms` | none |
| `projections.lag.pipeline.projection.max` | Observable gauge of long | `ms` | none |
| `projections.lag.pipeline.average` | Observable gauge of long | `ms` | none |
| `projections.lag.pipeline.projection.average` | Observable gauge of long | `ms` | none |
| `projections.workingset.size` | Observable gauge of long | none | `projection.type` |

Flush duration, flush view count, flush errors, and checkpoint lag record
when `EnableFlushMetrics` is on. Read total, cache hit, and cache miss
record when `EnableCacheMetrics` is on. The `source` tag on
`projections.read.total` is `memory` or `sql`, and only when
`EnableHighDetailTags` is on. Stale rejects, working-set size, and
working-set evictions record when `EnableExtendedMetrics` is on. The
eviction `reason` tag is added when `EnableHighDetailTags` is on. The
caller passes `max_instances` when it omits a reason.

### LawnDart.Authorization

Package: `LawnDart`.

| Name | Type | Unit | Tags |
|---|---|---|---|
| `authorization.check.duration` | Histogram of double | `ms` | none |
| `authorization.check.result` | Counter of long | `count` | `result` |
| `authorization.permission.denied` | Counter of long | `count` | `permission` |
| `authorization.entitlement.denied` | Counter of long | `count` | `entitlement` |
| `authorization.context.missing` | Counter of long | `count` | `provider_type` |

`result` is `success` or `failure`.

## Activity sources

Span kind is Internal unless the table says otherwise. A tag is set when
the code calls `SetTag` for that span.

The code adds two span events, both from `LawnDart.Messaging`:

| Event | Tags | Also |
|---|---|---|
| `messaging.retry` | `messaging.attempt` | Counted on `messaging.delivery.retries` |
| `messaging.deadlettered` | `messaging.attempt`, `error.type` | Span status Error. Counted on `messaging.deadlettered` |

Both events are added to the current activity. The transports add them
while the consume span is current.

### LawnDart.Messaging

| Span | Tags |
|---|---|
| `Messaging.Publish` | `messaging.message_type`. RabbitMQ also sets `messaging.rabbitmq.exchange` and `messaging.rabbitmq.routing_key`. Azure Service Bus also sets `messaging.servicebus.topic`. A thrown publish sets status Error. |
| `Messaging.Consume` | `messaging.message_type`, `messaging.message_id`. RabbitMQ also sets `messaging.rabbitmq.queue` and `messaging.rabbitmq.redelivered`. Azure Service Bus also sets `messaging.servicebus.topic`, `messaging.servicebus.subscription`, `messaging.servicebus.delivery_count`, and `messaging.servicebus.redelivered`. |

### LawnDart.EventProcessor

| Span | Kind | Tags |
|---|---|---|
| `EventProcessor.Process` | Consumer | `event_processor.type`, `event_processor.event_type`, plus `correlation_id` and `messaging.message_id` when the message context has them |

The span continues from an inbound `traceparent` header when that header
is present. `traceparent` is the W3C (World Wide Web Consortium) trace
header.

### LawnDart.Reactor

| Span | Kind | Tags |
|---|---|---|
| `Reactor.Handle` | Consumer | `reactor.type`, `reactor.event_type`, plus `correlation_id` and `messaging.message_id` when the message context has them |

This span continues from `traceparent` the same way.

### LawnDart.TaskProcessor

| Span | Tags |
|---|---|
| `TaskProcessor.Poll` | `task_processor.type` |

### LawnDart.EventStore

| Span | Tags |
|---|---|
| `EventStore.Append` | `lawndart.stream_id`, `lawndart.event_count` |
| `EventStore.Read.` plus the operation string | `lawndart.stream_id` when a stream id is passed |

### LawnDart.Dcb

| Span | Tags |
|---|---|
| `DcbGetState` | `state.type`, `tags.count` |
| `DcbAppend` | `event.count` |
| `DcbHandleCommand` | `command.type` |

### LawnDart.Projections

| Span | Tags |
|---|---|
| `ProcessEvent` | `projection.type`, `projection.instance`, `event.type` |
| `PollEvents` | `projection.type`, `node.instance` |

Both spans use kind Internal.

### LawnDart.Authorization

| Span | Tags |
|---|---|
| `AuthorizationCheck` | `command.type` |

### LawnDart.CommandDispatcher

Package: `LawnDart.EventSourcing`. This source has no meter.

| Span | Kind | Tags |
|---|---|---|
| `command.` plus the command's short type name | Internal | `correlation_id` and `messaging.message_id` when the message context has them |

The span continues from `traceparent` when that header is present.

### LawnDart.AspNetCore

Package: `LawnDart.AspNetCore`. This source has no meter.

| Span | Kind | Tags |
|---|---|---|
| `http.command.` plus the command's short type name | Server | `correlation_id` and `messaging.message_id` when the message context has them |

The span continues from the HTTP `traceparent` header when that header is
present.

## Related

- [Message transports](MESSAGE_TRANSPORTS.md)
- [Messaging package](packages/messaging.md)
- [Outbox](OUTBOX_PATTERN.md)
