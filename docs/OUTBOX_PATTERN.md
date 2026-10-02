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

Pair the outbox with `LawnDart.Messaging.InMemory` for single-process tests,
or your own `IMessageTransport` implementation.

## Dead letters

When publish attempts reach the processor max (default 10), the row is marked
with `DeadLetteredAt` and dropped from `GetUnprocessedAsync`. Query failed rows
with `GetDeadLetteredAsync`. `ProcessedAt` is success only; dead letters do not
set it. Inspect those rows in place. This version has no reset or replay API.
