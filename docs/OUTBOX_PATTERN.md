# Outbox pattern

Use the transactional outbox when a consumer must see an event that committed
with the append. SQL Server and Postgres write the outbox row in that
transaction. The in-memory store records the same message in the process when
`EnableOutbox` is true. A consumer in another process reads a SQL Server or
Postgres row.

## When

- The consumer is another process: `UseSqlServer` or `UsePostgres`, with `EnableOutbox`
- The consumer is in this process and should read the outbox: `UseInMemory` with `EnableOutbox`

Academy's in-memory profile delivers with `InMemoryMessageTransport` and leaves
`EnableOutbox` false.

## Declared events

An event leaves the context only when `PublishEvents` declares it. Call it
after `WithEventTypes`.

`Translate<TInternal, TPublic>` is the normal case. The stored event stays
internal. The outbox row is a separate integration event. The translation is
pure and synchronous, runs inside the append, and may read only the event and
`PublishContext`. Return null to publish nothing for that event. One internal
event maps to at most one published event.

`Publish<T>()` sends the event as stored. Use it when that event already lives
in a contracts project.

There is no publish-everything option.

```csharp
[EventTypeName("counter-incremented-notice")]
public sealed record CounterIncrementedNotice(
    Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;

services.AddBoundedContext("default")
    .WithEventTypes(typeof(CounterCreated), typeof(CounterIncremented))
    .PublishEvents(events => events
        .Translate<CounterIncremented, CounterIncrementedNotice>((incremented, publication) =>
            new CounterIncrementedNotice(
                publication.SourceEventId,
                incremented.Timestamp,
                incremented.CounterId)))
    .UseSqlServer(o =>
    {
        o.ConnectionString = cs;
        o.EnableOutbox = true;
    });

services.AddMessageTransportOutboxPublisher();
```

`Publish<CounterCreated>()` is the pass-through form, for an event that
already lives in a contracts project. `UsePostgres` and `UseInMemory` take the
same `EnableOutbox` flag. `WithUpcasters<TMarker>()` inside `PublishEvents`
supplies older versions of a published event, so an outbox row written earlier
can still be read. Upcasters for internal events are ignored on that read.

`AddTranslator<T>()` and `AddTranslators<TMarker>()` register the same
translations as classes. A translator has a public parameterless constructor
and cannot take services.

The store fails to open when `EnableOutbox` is true and the context declares
nothing. The message names the context and lists its event types.
`PublishedEventMap.NothingDeclaredMessage` is that text. A store outside this
repository can throw the same text.

`PublishedEventCatalog` is one list for the process, the union of every
context's published types. Two contexts that publish one event name with
different .NET types fail when the second context is registered.

A raw `IEventLog` append writes a row only when the caller sets
`AppendEvent.Outbound`. The store does not translate that append.

The outbox row id is the source event id. That id is the message id.
`MessageTransportOutboxPublisher` adds the header `outbox.sourceEventId` with
the same value.

The row's metadata is reduced. It keeps the source event id, tenant,
correlation, causation, trace id, span id, the source event's business time,
and the notice's event name and schema version. `Custom` holds only
`lawndart.source-event-id` and `lawndart.source-event-type`. User, network,
account, and authorization fields are not copied. A notice that needs a user
id carries it as a field.

`AddMessageTransportOutboxPublisher()` with no types reads
`PublishedEventCatalog`. Pass types when that catalog is not registered.

## Moving to declared events

Declare each event that should leave the context. Move an internal event onto
`Translate`. Move an event that already lives in a contracts project onto
`Publish`. Then set `EnableOutbox`.

A context that declares nothing fails when the store opens. The message lists
that context's event types.

A program already compiled against the six-argument `AppendEvent` constructor
must be rebuilt. Source that calls `AppendEvent` still compiles. The
constructor takes an optional `OutboundEvent` at the end.

## Shape

1. Append domain events and, for each declared event, an outbox row in one commit. SQL Server and Postgres use one transaction. The in-memory store records both in the process.
2. A background publisher reads unpublished rows. It waits on the store's
   new-events signal or its poll interval, whichever comes first. A committed
   save in this process wakes it. The poll interval still publishes rows when
   no signal arrives.
3. `AddMessageTransportOutboxPublisher` hands payloads to `IMessageTransport`.

The outbox processor counts `outbox.publish.failures` on the `LawnDart.Outbox`
meter when a publish throws, and `outbox.deadlettered` when it parks a row
that used its attempts. Both are tagged with `outbox.event_type`.

The processor runs as a background service. When its loop fails, the host
applies `HostOptions.BackgroundServiceExceptionBehavior`. The default,
`StopHost`, logs the failure and stops the host. `Ignore` logs the failure,
and the host keeps running without publishing outbox rows.

With more than one `IOutboxPublisher` registered, the processor calls them in
registration order, and each row goes to each publisher once. When a publisher
throws, the row records the publishers that already published it in
`PublishedTo`, one bit per publisher, and counts one attempt. The next attempt
calls only the publishers that have not published the row. A dead-lettered row
keeps `PublishedTo`, and so does a reset. A context supports up to 64
publishers. Keep the registered publishers, and their order, the same while
rows wait to be published.

The outbox row id is the source event id. `GetUnprocessedAsync` claims each row
for `OutboxClaimLease` (default 30 seconds). On SQL Server the deadline is
`ClaimedUntil`, taken with `UPDLOCK, READPAST`. On Postgres the deadline is
`claimed_until`, taken with `FOR UPDATE SKIP LOCKED`, and the claimed rows
come back in position order. A second processor does not publish that row.
Set the lease to `TimeSpan.Zero` when the next poll should claim the row again.
A failed publish clears the claim. On SQL Server, format `1` and `2` outbox
tables gain `ClaimedUntil` and `PublishedTo` when `InitializeSchemaAsync` runs.
On Postgres, `InitializeSchemaAsync` adds the `outbox` table when it is missing,
and adds `published_to` to a format 1 outbox table. The events
format stamp stays at 1. A process still running 0.7.0-alpha.1 can keep
appending. It does not write outbox rows, and the store does not report that.
Deploy the new version with `EnableOutbox` false until no 0.7.0-alpha.1 copy
can run. An older copy that restarts can run its own schema setup at startup.
Run schema setup from your deploy step instead, and make sure no older copy
can start before turning on `EnableOutbox`. See
[Schema initialization](SCHEMA_INITIALIZATION.md).
On a brand-new database during a mixed-version deploy, initialize the event
store with the new version first, because the 0.7.0-alpha.1 event store cannot
initialize a schema that the new inbox created first. The
in-memory writer uses the same lease option.

Postgres details are on the [Postgres event store page](packages/eventsourcing-postgres.md#outbox).

The outbox row is the integration event, written with the append: event
name, payload **bytes**, reduced metadata as UTF-8 JSON, plus `SchemaVersion`
and the payload-format column `CodecId` (`TINYINT`, no column default). SQL
Server and Postgres write it in the append transaction. A rolled-back append
leaves no row. The in-memory store records the same message in the process.
The publisher picks the payload format from `CodecId` and reads the row
through `EventSession` (event name plus schema version, then upcast). The
catalog for that read is `PublishedEventCatalog`, including upcasters declared
inside `PublishEvents`. A name that is not in that catalog makes the publisher
throw. A .NET `FullName` is not accepted in its place. Drop and recreate an
older outbox table: `SchemaVersion` and `CodecId` have no column defaults, and
opening that table throws and names drop-and-recreate.

Register one transport before the publisher. The publisher hands each row
to the registered `IMessageTransport`.

In-process, for a single process:

```csharp
services.AddInMemoryMessaging();
services.AddMessageTransportOutboxPublisher();
```

RabbitMQ:

```csharp
services.AddRabbitMqTransport(options =>
{
    options.Uri = "amqp://rabbitmq:rabbitmq@localhost:5672/";
    options.SubscriptionName = "billing";
});
services.AddSqlInboxStore(cs);
services.AddMessageTransportOutboxPublisher();
```

Azure Service Bus:

```csharp
services.AddAzureServiceBusTransport(options =>
{
    options.ConnectionString = cs;
    options.SubscriptionName = "billing";
});
services.AddSqlInboxStore(cs);
services.AddMessageTransportOutboxPublisher();
```

Amazon SQS:

```csharp
services.AddAwsSqsTransport(options =>
{
    options.Region = "us-east-1";
    options.SubscriptionName = "billing";
});
services.AddSqlInboxStore(cs);
services.AddMessageTransportOutboxPublisher();
```

`DeadLetterFailedMessages` defaults to false. A publish that throws is a
failure the outbox processor records. The row stays unpublished. The
processor tries it again until its own limit (default 10), then marks the
row with `DeadLetteredAt`. That processor limit is separate from the
transport flag.

The outbox processor waits until the host has started every hosted service
before the first poll. A reactor registered before or after the processor
is subscribed by then. On shutdown the processor stops taking new rows
before listeners unsubscribe. It finishes the row already in flight and
leaves later rows unprocessed. That wait uses the host shutdown timeout.
When the timeout fires, the processor logs a warning and shutdown continues.
If the processor never started, shutdown does not wait and does not log
that warning.

On the in-process transport, the handler runs inside the publish call.
A publish with no in-process subscriber throws. The outbox keeps the row
and tries it again until the attempt limit (default 10), then marks the
row with `DeadLetteredAt`. After `MaxDeliveryAttempts` the last handler
exception is thrown, so the outbox keeps the row. Set
`DeadLetterFailedMessages` to true when the transport should park the
message and return. The outbox then marks the row processed.

A listener is one of these:

- In-process: a subscriber registered in this process. When that subscriber
  stops, the next publish has no listener.
- RabbitMQ: a queue bound to the event type, so a mandatory publish is not
  returned. A queue another service owns counts. A stopped consumer whose
  queue still exists still receives.
- Amazon SQS: a live queue subscription on the topic. The transport remembers
  a topic after it has seen a queue there, for the life of the process.
  Removing the last queue later is not detected until the process starts
  again. A publish after that removal is accepted, and the outbox marks the
  row processed. A queue another service owns counts. A stopped consumer
  whose queue subscription still exists still receives.

`UnroutablePublish` defaults to `Throw` on RabbitMQ and Amazon SQS. The
publish throws, and the outbox keeps the row. Set `UnroutablePublish` to
`Warn` to log and return. `Warn` does not queue the message, and the outbox
marks the row processed.

The processor polls every 5 seconds by default and allows 10 attempts.
There is no extra delay between attempts. A new append can wake the
processor sooner. A row with no listener is marked `DeadLetteredAt` after
about 10 polls, roughly 50 seconds with those defaults. Reset one row with
`ResetDeadLetteredAsync`, or every dead-lettered row with
`ResetAllDeadLetteredAsync`. The processor publishes it again on the next
poll. See [Dead letters](#dead-letters).

Startup checks each published event type for a listener before that first
poll. A type registered with `AddReactor` or `AddEventProcessor` counts as
a listener. The check does not ask the transport about that type. The check
runs for the in-process transport and for Amazon SQS. RabbitMQ and Azure
Service Bus do not report listeners at startup. The host logs that and
continues. When startup is set to stop on a missing listener, that log is
a warning. The publish-time check is the safety net there. Amazon SQS lists
topics and subscriptions for a published type this process does not
subscribe to. That list needs permission to list them. When the list fails,
the host logs that and continues.
`MessageTransportOptions.OnMissingListenerAtStartup` defaults to `Warn`.
`Warn` logs one warning for each type the check finds with no listener, and
the host still starts. `Throw` stops host startup when the check finds a
type with no listener. `Throw` does not stop startup when the check cannot
tell. The outbox keeps an unpublished row and tries it again until the
attempt limit.

On Azure Service Bus, a publish the broker rejects throws, and the outbox
keeps the row. A handler failure after the broker accepted the message is
the transport's retry. With the flag left false, that message is not parked.
See [Message transports](MESSAGE_TRANSPORTS.md).

Delivery to the transport is at least once. A consumer that must ignore a
redelivery after a restart stores that fact in SQL Server:

```csharp
services.AddInMemoryMessaging();
services.AddSqlInboxStore(cs);
```

Call `InitializeSqlInboxStoreAsync` before hosted reactors start. The
in-memory inbox is per process. Each reactor or processor deduplicates on
its own key (`reactor:{hash}:{message id}` or `processor:{hash}:{message id}`).
That hash is taken from the consumer type's full name, so two consumers of
one message both run once.

## Dead letters

When publish attempts reach the processor max (default 10), the row is marked
with `DeadLetteredAt` and dropped from `GetUnprocessedAsync`. `ProcessedAt`
is success only. A dead letter leaves `ProcessedAt` null.

List failed rows with `GetDeadLetteredAsync`. The writer is keyed by
bounded-context name. Reset one row, or every dead-lettered row, and the
processor publishes it again on the next poll:

```csharp
var writer = sp.GetRequiredKeyedService<IOutboxWriter>("default");
var dead = await writer.GetDeadLetteredAsync(100);
await writer.ResetDeadLetteredAsync(dead[0].Id);
// or: var reset = await writer.ResetAllDeadLetteredAsync();
```

Reset clears `DeadLetteredAt` and sets `Attempts` to 0. `LastError` and
`LastAttemptAt` stay until the next attempt overwrites them. An unknown id,
a row that was never dead-lettered, and an already processed row each return
`false` from `ResetDeadLetteredAsync` and stay as they were.
`ResetAllDeadLetteredAsync` returns how many rows changed.

The republished row keeps the same id.
`MessageTransportOutboxPublisher` uses that id as
`MessageContext.MessageId`. A consumer whose inbox already marked that id
processed inside the dedup window drops the delivery. That is the expected
at-least-once behavior. Reset after the window expires when the consumer
must run again.
