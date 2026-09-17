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

The outbox row is a copy of the appended frame: family token, payload and
metadata as UTF-8 JSON text, plus first-class `SchemaVersion` and
`CodecId`. The publisher resolves the CLR type through
`IEventTypeCatalog` (token + schema version), then upcasts through
`EventSession` the same way typed store reads do. An unknown token fails
closed; there is no FullName alias.

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
with `GetDeadLetteredAsync`. `ProcessedAt` is success only — dead letters do not
set it. There is no reset/replay API in this version.
