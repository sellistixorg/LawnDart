# LawnDart.Messaging.SqlServer

SQL Server `IInboxStore`. Reactor and event-processor deduplication survives
a process restart and is shared by every instance that uses the same table.

The transport stays in-process (`LawnDart.Messaging.InMemory`) unless you
register your own `IMessageTransport`.

## Registration

```csharp
services.AddInMemoryMessaging();
services.AddSqlInboxStore(connectionString);
```

`AddSqlInboxStore` replaces the in-memory inbox, whether you call it before
or after `AddInMemoryMessaging`. `AddInMemoryMessaging` still registers the
transport.

After the host is built, create the table once:

```csharp
await app.Services.InitializeSqlInboxStoreAsync();
```

## Table

`MessageId` is `NVARCHAR(256)`, the primary key. `ProcessedAt` is
`DATETIMEOFFSET(7)`. A nonclustered index covers purge by time.

`IsProcessedAsync` returns true only when `ProcessedAt` is inside
`MessagingOptions.InboxDeduplicationWindow` (default 24 hours).
`MarkProcessedAsync` is idempotent. `PurgeExpiredAsync` deletes rows older
than that window and returns the count.

Hosted reactors store `reactor:{type}:{message id}`. Hosted processors
store `processor:{type}:{message id}`. Two consumers of one message each
keep their own row. The key must be 256 characters or fewer.

An existing table with the wrong shape throws
`IncompatibleInboxSchemaException`. Drop and recreate the table. There is
no in-place migration.

Defaults are schema `dbo` and table `Inbox`. Pass a configure delegate to
`AddSqlInboxStore` to change them.

## Related

- [Package map](README.md)
- [Messaging](messaging.md)
- [InMemory messaging](messaging-inmemory.md)
- [Outbox](../OUTBOX_PATTERN.md)
- [EDA patterns](../EDA-Patterns.md)
