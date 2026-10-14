# Outbox pattern

Use the transactional outbox when the event store is SQL Server and you must
publish to a message transport without dual-write races.

## When

- Durable store (`UseSqlServer`)
- A reactor or downstream consumer lives in another process (or will)

Skip the outbox for Academy InMemory: `InMemoryMessageTransport` is already
in-process.

## Shape

1. Append domain events and outbox rows in one SQL transaction.
2. A background publisher reads unpublished rows.
3. `AddMessageTransportOutboxPublisher` hands payloads to `IMessageTransport`.

The outbox processor counts `outbox.publish.failures` on the `LawnDart.Outbox`
meter when a publish throws, and `outbox.deadlettered` when it parks a row
that used its attempts. Both are tagged with `outbox.event_type`.

The outbox row is a copy of the appended frame, written in the same
transaction: family token, payload **bytes**, metadata as UTF-8 JSON
text, plus first-class `SchemaVersion` and `CodecId` (`TINYINT`, no
column default). The publisher selects the codec by id and hydrates
through `EventSession` (token + schema version, then upcast) the same
way typed store reads do. Register the family with `WithEventTypes`; an
unknown token fails closed (no FullName alias). Drop and recreate an
older outbox table: `SchemaVersion` and `CodecId` have no column defaults,
and opening that table throws and names drop-and-recreate.

```csharp
services.AddBoundedContext("default")
    .UseSqlServer(o =>
    {
        o.ConnectionString = cs;
        o.EnableOutbox = true;
    });

services.AddMessageTransportOutboxPublisher();
```

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

`DeadLetterFailedMessages` defaults to false. A publish that throws is a
failure the outbox processor records. The row stays unpublished. The
processor tries it again until its own limit (default 10), then marks the
row with `DeadLetteredAt`. That processor limit is separate from the
transport flag.

On the in-process transport, the handler runs inside the publish call.
After `MaxDeliveryAttempts` the last exception is thrown, so the outbox
keeps the row. Set `DeadLetterFailedMessages` to true when the transport
should park the message and return. The outbox then marks the row processed.

On RabbitMQ and Azure Service Bus, a publish the broker rejects throws, and
the outbox keeps the row. RabbitMQ's default for a publish that reaches no
queue is `UnroutablePublish` `Warn`: the call returns, the message was not
queued, and the outbox marks the row processed. Set `UnroutablePublish` to
`Throw` when that case should stay on the outbox. A handler failure after
the broker accepted the message is the transport's retry. With the flag left
false, that message is not parked. See
[Message transports](MESSAGE_TRANSPORTS.md).

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
