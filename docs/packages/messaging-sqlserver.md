# LawnDart.Messaging.SqlServer

SQL Server `IInboxStore`. Reactor and event-processor deduplication survives
a process restart and is shared by every instance that uses the same table.

Pair it with the in-process transport (`LawnDart.Messaging.InMemory`),
RabbitMQ (`AddRabbitMqTransport`), or Azure Service Bus
(`AddAzureServiceBusTransport`). This package replaces the inbox. The
transport you registered stays.

## Registration

```csharp
services.AddInMemoryMessaging();
services.AddSqlInboxStore(connectionString);
```

`AddSqlInboxStore` replaces the inbox, whether you call it before or after
the transport registration. The snippet above uses the in-process transport.
`AddRabbitMqTransport` and `AddAzureServiceBusTransport` keep their transport
the same way.

After the host is built, create the table once:

```csharp
await app.Services.InitializeSqlInboxStoreAsync();
```

## Table

`MessageId` is `NVARCHAR(256)`, the primary key. `ProcessedAt` is
`DATETIMEOFFSET(7)`. A nonclustered index covers purge by time.

`IsProcessedAsync` returns true only when `ProcessedAt` is inside
`MessagingOptions.InboxDeduplicationWindow` (default 24 hours).
`TryClaimAsync` holds the key for `InboxClaimLease` (default 30 seconds).
`TimeSpan.Zero` lets the next caller claim it at once. A failed handler
releases the claim. `MarkProcessedAsync` is idempotent. `PurgeExpiredAsync`
deletes rows older than the deduplication window and returns the count.

Hosted reactors store `reactor:{hash}:{message id}`. Hosted processors
store `processor:{hash}:{message id}`. `hash` is 32 hexadecimal characters
from SHA-256 of the consumer type's full name, so a long or generic name
stays inside the column. Two consumers of one message each keep their own
row. The key must be 256 characters or fewer. A message id that would push
the key over that limit is stored as a hash of the same length.

An existing table with the wrong shape throws
`IncompatibleInboxSchemaException`. Drop and recreate the table. There is
no in-place migration.

Defaults are schema `dbo` and table `Inbox`. Pass a configure delegate to
`AddSqlInboxStore` to change them.

## Related

- [Package map](README.md)
- [Messaging](messaging.md)
- [InMemory messaging](messaging-inmemory.md)
- [RabbitMQ](messaging-rabbitmq.md)
- [Azure Service Bus](messaging-azureservicebus.md)
- [Outbox](../OUTBOX_PATTERN.md)
- [EDA patterns](../EDA-Patterns.md)
