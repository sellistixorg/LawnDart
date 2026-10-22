# LawnDart.Messaging.Postgres

Postgres `IInboxStore`. Reactor and event-processor deduplication survives
a process restart and is shared by every instance that uses the same table.

Pair it with the in-process transport (`LawnDart.Messaging.InMemory`),
RabbitMQ (`AddRabbitMqTransport`), Azure Service Bus
(`AddAzureServiceBusTransport`), or Amazon SQS (`AddAwsSqsTransport`).
This package replaces the inbox. The transport you registered stays.

## Registration

```csharp
services.AddInMemoryMessaging();
services.AddPostgresInboxStore(connectionString);
```

`AddPostgresInboxStore` replaces the inbox, whether you call it before or
after the transport registration. The snippet above uses the in-process
transport. `AddRabbitMqTransport`, `AddAzureServiceBusTransport`, and
`AddAwsSqsTransport` keep their transport the same way.

After the host is built, create the table once:

```csharp
await app.Services.InitializePostgresInboxStoreAsync();
```

## Table

The default schema is `lawndart`. Any other schema name is set on
`PostgresInboxStoreOptions.SchemaName`. The `public` schema is rejected.
The table name defaults to `inbox`.

`message_id` is text, the primary key, at most 256 characters.
`processed_at` and `claimed_at` are timestamps. An index covers purge by time.

`IsProcessedAsync` returns true only when `processed_at` is inside
`MessagingOptions.InboxDeduplicationWindow` (default 24 hours).
`TryClaimAsync` is one insert that updates the existing row only when the
key is outside that window and has no live claim. The claim lasts for
`InboxClaimLease` (default 30 seconds). `TimeSpan.Zero` lets the next caller
claim the key at once. A failed handler releases the claim. `MarkProcessedAsync`
is idempotent. `PurgeExpiredAsync` deletes rows older than the deduplication
window and returns the count.

`TryClaimAsync` is one statement, so it runs through PgBouncer in transaction
mode. PgBouncer is a connection pooler that can sit in front of Postgres.

Hosted reactors store `reactor:{hash}:{message id}`. Hosted processors
store `processor:{hash}:{message id}`. `hash` is 32 hexadecimal characters
from SHA-256 of the consumer type's full name, so a long or generic name
stays inside the column. Two consumers of one message each keep their own
row. The key must be 256 characters or fewer. A message id that would push
the key over that limit is stored as a hash of the same length.

When the table is created, autovacuum uses a vacuum scale factor of 0.01, a
vacuum threshold of 1000, an analyze scale factor of 0.02, and an analyze
threshold of 1000. `InitializePostgresInboxStoreAsync` adds the table when
it is missing. The format stamp is component `inbox` in `schema_info`, format
1. A different stamp throws `IncompatibleInboxSchemaException` and names drop
and recreate.

## Related

- [Package map](README.md)
- [Messaging](messaging.md)
- [Postgres event store](eventsourcing-postgres.md)
- [Outbox](../OUTBOX_PATTERN.md)
