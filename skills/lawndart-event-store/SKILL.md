---
name: lawndart-event-store
description: Choose and register a LawnDart event store. UseInMemory for zero infra; UseSqlServer for durable SQL. Use when picking or swapping the event-store backend.
---

# Event store

v1 backends: **InMemory** and **SQL Server**. Both persist recorded frames
(`IEventLog`: `AppendEvent` in, `RecordedEvent` out). Application code uses
the typed session (`IEventStore`). InMemory serializes on append. It is
not an object heap. `IEventSerializer` reads and writes `ReadOnlyMemory<byte>`
(UTF-8 JSON by default). Third-party stores implement the log, not the session.

`WithEventTypes` is required. Frames store a `CodecId`, not a MIME string.
SQL uses one `EventData` (`VARBINARY`) column. Pre-1.0 schema changes are
a wipe: drop and recreate event and outbox tables. There is no in-place
migration.

SQL does not create tables on first append. Call
`SqlServerEventStore.InitializeSchemaAsync` before the first command.
See `lawndart-host-setup` for snapshots and projection-store init.

Additive JSON rolls freely. A breaking payload change is expand-contract.
Register hops with `WithUpcasters` after `WithEventTypes`. See
`docs/EVENT_SCHEMA_VERSIONING.md`.

Excerpt from `samples/Library.Host/LibraryHost.cs` (`AddInMemoryLibrary`):

```csharp
var ctx = services.AddBoundedContext("default");
ctx.UseInMemory();
ctx.WithEventTypes<BookAdded>();
```

Excerpt from `samples/Library.Host/LibraryHost.cs` (`AddSqlLibrary`):

```csharp
services.AddInMemoryMessaging();
services.AddSqlInboxStore(connectionString);
services.AddMessageTransportOutboxPublisher();
services.AddReactor<LoanNoticeReactor, BookBorrowed>();
services.AddSqlProjectionStores("default", connectionString);
var ctx = services.AddBoundedContext("default");
ctx.UseSqlServer(o =>
{
    o.ConnectionString = connectionString;
    o.RequireTenantId = false;
    o.EnableOutbox = true;
});
ctx.WithCommandHandlers<BorrowBookHandler>();
ctx.WithEventTypes<BookAdded>();
ctx.WithProjections(
    [typeof(LibraryCatalogProjection).Assembly],
    opts =>
    {
        opts.PollInterval = TimeSpan.FromMilliseconds(200);
        opts.CheckpointInterval = 100;
    });
```

Call `InitializeSqlLibraryAsync` before the first command. It creates the
event-store, outbox, projection, and inbox tables.

Align `RequireTenantId` on `AddLawnDart` and `SqlServerEventStoreOptions`.

See `docs/BACKEND_SELECTION.md`.
