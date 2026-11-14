# LawnDart.EventSourcing.SqlServer

Durable SQL Server implementation of `IEventLog` plus the typed `IEventStore`
session. Same repositories and domain types as InMemory; only the backend
changes. The log stores records (event name, schema version,
payload-format number, payload bytes, metadata JSON). The session
serializes on append and reads them back into objects.

Take it when InMemory is no longer enough. Keep the same bounded-context
name so projections and HTTP mapping stay pointed at the same stack.

## Payload layout

One payload column: `EventData VARBINARY(MAX) NOT NULL`. `SchemaVersion`
(`INT`), `CodecId` (`TINYINT`), and `EventTypeId` (`INT`) are `NOT NULL`
with no defaults. The session stamps all three on append. Event names
live in an `EventTypes` lookup (`Id INT IDENTITY`, `Token NVARCHAR(500)`
unique). `Events` stores `EventTypeId` and a foreign key; the store
caches each event name and its id both ways at schema init so reads do
not join. A cache miss reloads `EventTypes` once. If the id is still
missing, the read throws and names drop-and-recreate. It does not invent
an event name.

Type filters are `EventTypeId IN (...)`. A queried event name that is not in
the cache matches zero rows. A query whose names are all absent does not
emit SQL. `IEventLog.AppendAsync`
still accepts an event name that is not in the lookup yet: get-or-insert runs in its
own short transaction and commits before the append transaction opens.
An orphan `EventTypes` row is harmless.

`Tags` is `NVARCHAR(4000)` and is a read projection only. DCB queries
and append-condition checks use the `EventTags` side table (clustered on
`(Tag, GlobalSequencePosition)`, collation `Latin1_General_100_BIN2`).
Tag equality is ordinal. `student:{id}` and `Student:{id}` are different
tags. That table is always created and always written. See
[Tagging](../TAGGING.md). There is no
`UseEventTagsTable` flag and no `OPENJSON` fallback on `Events.Tags`.
Metadata stays `NVARCHAR` JSON.

A subscription read asks for one page (`SqlSubscriptionBatchSize`) up to
the gap-free head. The scan cursor then moves to the highest position
examined, including positions that did not match the filter. A failed
read completes the subscription with `EventSubscriptionFailedException`.
Cancel and dispose still end it without an error.

After a save in this process commits, the store signals the newest global
position (`ISignalsNewEvents`). The subscription takes a ticket before it
reads. It then waits on that ticket or `SqlSubscriptionPollInterval`,
whichever comes first, and reads again from its checkpoint. A save that
commits after the ticket wakes it at once. The signal carries no events.
When that position is still ahead of the gap-free head, the subscription
reads again on a short retry. The open transaction that hides the row does
not signal when it ends. A save from another process is still picked up on
the poll interval once the head has caught up. A listener outside the store
can raise the same signal.

A type filter reloads `EventTypes` when a name is missing from the
process cache. The current end is taken before that reload. A name that is
still absent matches no rows, and this read stops at that end. Events for
the other names are still returned. `TypesResolved` is false when any
requested name is still absent. Events up to the last one returned are
delivered. The position does not move past them to the current end until
every requested name is stored. A projection reads the range again on the
next poll.

Each appended record has an `EventId` (`UNIQUEIDENTIFIER`). The pair
(`TenantId`, `EventId`) is unique. The same id on another tenant is a
different row. A typed append stores `IEvent.Id`, including when several appends share one
`EventMetadata` object. A raw append uses metadata `EventId` when that
value is a GUID. Otherwise the store mints one before the commit and
reuses it if the commit is retried inside the same call. A second append
of the same id in that tenant throws `EventAlreadyAppendedException`. The outbox row
uses that same id.

The store does not retry an append after `Commit` has been attempted. When
the commit reply is lost, it reads those event ids. If they are present, the
append result is the row that landed.

Every append takes named SQL Server locks (`sp_getapplock`, owned by the
transaction) before it reads or writes anything, and holds them until it
commits or rolls back. They live only in SQL Server's lock memory and read
no table pages:

- One store lock per events table. Every append takes it shared.
- One lock per tag. An append takes a shared lock on each tag it writes.
- An append with an `AppendCondition` takes an exclusive lock on each tag in
  the condition. When the condition has no items, or any item has no tags
  (event types only, or `Query.All()`), it takes the store lock exclusive.
  That append waits for every append in progress, and new appends wait for
  it, the same as on Postgres. The DCB repository never builds that shape.
- An append with more distinct tags than `AppendLockTagThreshold` (default
  64) takes the store lock exclusive and no tag locks.

The locks, the condition or version check, and the inserts go to SQL Server
as one batch. The batch reserves every event position with one call to
`sp_sequence_get_range`. Commit is the next call. The expected version check
and the insert both use the stream version read while that stream row is locked.

The store lock comes first, then the tag locks sorted by name, so appends
always ask in the same order. Lock names are a short prefix and a hash of
the schema, the events table and the tag, so any tag length fits. Two names
with the same hash only make two appends wait for each other. A lock that is
not granted is retried like a deadlock (`MaxRetryCount`). The condition
check runs after the locks as a plain read. Any event that matches it was
committed before the locks were granted, or its append is still waiting for
them. After the locks, the check also looks up its event type names again,
so an event whose type was stored for the first time just before is seen.

## Events table

`Events` is clustered on `SequencePosition`, the global position. A catch-up
read walks that order.

Each row stores `StreamKey` (`BIGINT`) instead of the stream id text. The
stream registry is required. `Streams` maps `StreamId` to `StreamKey`. A
stream load reads the index `IX_Events_StreamKey_Version` on
(`StreamKey`, `Version`).

`TenantId` is stored on the event. `UX_TenantId_EventId` keeps one `EventId`
per tenant. `EnableStreamRegistry` and `AutoMaintainRegistry` are still
accepted. The registry is maintained either way. An append locks that
stream's registry row until it commits, so a second append of the same
stream waits. An append of a different stream does not wait on that row.
When both name the same expected version, the one that waits throws
`ConcurrencyException` after the first commits. The stream keeps that first
append.

`EventSnapshots` uses that same `StreamKey`. One row per stream.

`Events_Readable` still shows the stream id text. It joins `Streams` for
the id and `EventTypes` for the event name.

Init writes an extended property `LawnDart_SchemaFormat` on the events
table (current format `5`). A table with no stamp, or with any other stamp,
is not changed. The call throws and names drop and recreate. Back up first.
See [Upgrading from 0.6.0-alpha.2](#upgrading-from-060-alpha2).

`RowVersion`, `AppendFence`, and `EventTags.Tag` at `Latin1_General_100_BIN2`
are part of a new format 5 table.

`RowVersion` is the commit horizon. An append inserts an `AppendFence` row
before it reserves a sequence, so the transaction owns a rowversion first.
A later append removes fence rows whose transaction has already committed.
Global reads and `GetCurrentSequenceAsync` include a row only when its
rowversion is below `MIN_ACTIVE_ROWVERSION()`. A sequence reserved by an
open append stays invisible while that append is open. A later committed
sequence stays invisible with it.

`ReadHorizonGracePeriod` (default 30 seconds) is how long one unfinished
write may hold that horizon. Thirty seconds covers a lock wait or a slow
batch. A stuck write then stops holding every reader. When the same write
is still open after the grace period, readers move to the highest committed
sequence and log a warning. The warning names the blocking session when
SQL Server reports it, and how long the write has been open. That name
comes from `sys.dm_tran_active_transactions`,
`sys.dm_tran_session_transactions`, and
`sys.dm_tran_database_transactions`. SQL Server and Azure SQL Managed
Instance need `VIEW SERVER STATE`. On Azure SQL Database, grant
`VIEW DATABASE STATE` on the database. Basic, S0, S1, and databases in
an elastic pool do not accept that grant for these views. Use the server
admin, the Microsoft Entra admin, or the `##MS_ServerStateReader##`
server role there. Without the permission the warning reports session
`unknown` and transaction `unknown`. The grace period still moves
readers to the committed rows. The hold time then starts when this
process first sees that stable head. Set the option to
`Timeout.InfiniteTimeSpan` to wait until the write ends. `TimeSpan.Zero`
moves readers forward once the write has been open for more than zero
milliseconds.

A row that commits below a head taken after that move is a skipped event.
The process that moved the horizon logs an error when a later check sees
the row. The watch list lives in that process. After a restart, or when
the process exits before the late commit, the late row is quiet. A read
whose window still starts at or below the sequence can still return the
row, and that read does not log. The in-memory store has no rowversion
horizon. A rolled-back sequence is a permanent gap. Readers continue at
the next committed row.

A transaction that writes any `rowversion` in this database holds the
horizon for every reader. Read-only work does not.

SSMS: `{table}_Readable` projects `CAST(EventData AS VARCHAR(MAX)) AS EventJson`
alongside the scalar columns and the event name from `EventTypes`.
Non-ASCII text needs a `_UTF8` collation on that cast (an operator
choice). The store does not detect server capability.

```sql
SELECT EventJson, EventType, EventTypeId, SchemaVersion, CodecId
FROM dbo.Events_Readable;
```

With `EnableOutbox` true, an append writes an outbox row for each event the
context declares with `PublishEvents`. The row is the integration event:
payload bytes (`VARBINARY`), payload-format number `CodecId` (`TINYINT`), and
`SchemaVersion` (`INT`), none of them with a column default. An undeclared
event is stored and gets no row. There is no publish-everything option.
Opening an older outbox table throws and names drop-and-recreate.
`EnableOutbox = true` writes those rows whether or not an `IOutboxWriter` was
injected. `OutboxClaimLease`
(default 30 seconds) is how long a claim hides a row. A row that exhausts
publish attempts is dead-lettered. `ResetDeadLetteredAsync` and
`ResetAllDeadLetteredAsync` clear that state on this context's table so
the processor publishes the row again. See
[Outbox pattern](../OUTBOX_PATTERN.md#dead-letters).

## Registration

```csharp
ctx.UseSqlServer(o =>
{
    o.ConnectionString = connectionString;
    o.RequireTenantId = false;
});
```

Registers the same store as keyed `IEventStore`, `IEventLog`, `IStreamRegistry`,
and `IEventStoreSubscriptions`.

Optional `WithSnapshots` and outbox (`EnableOutbox` on `EventSourcingOptions`,
which this options type inherits). Call `PublishEvents` after `WithEventTypes`
when the outbox is on. `Publish<T>()` sends an event that already lives in a
contracts project. `Translate` is the normal case. Snapshot write
durability is the same channel + hosted consumer as InMemory; see
[SNAPSHOTS.md](../SNAPSHOTS.md). Integration tests use Testcontainers and
are tagged `Category=Integration`. The shared contract
(`Category=Contract`) runs the same store checks on SQL Server. Schema
format, column layout, collation, identifier quoting, stream id length, and
multi-context schema setup stay in the SQL Server test project. See
[BACKEND_SELECTION.md](../BACKEND_SELECTION.md).

InMemory remains the documented default for local work and Academy.

## Stream pages

`ListStreamsAsync` reads active streams in stream id order. The order uses `Streams.StreamIdBin`, a persisted column defined as `StreamId` with the `Latin1_General_100_BIN2` collation, and a unique index on that column. A page without a tag reads that index from the last stream id. A prefix with no letters matches those exact characters, the same way the in-memory store and Postgres do. A full-width digit (U+FF11) or a superscript (U+00B2) is not a plain digit. That prefix is one range on `StreamIdBin`: the page starts at the later of the token and the prefix, and stops at the end of the range. A prefix that contains letters follows the `StreamId` column collation. A new database is case-insensitive. A case-sensitive database matches case. That prefix is not one range. The first page reads the clustered stream id key for that prefix, then orders those rows by `StreamIdBin`. A later page reads `StreamIdBin` from the token and applies the prefix, with no end bound. A page limited to an aggregate type reads the index on (`AggregateType`, `StreamIdBin`).

Tags for that list live in `StreamTags` (`Tag`, `StreamId`), with `Latin1_General_100_BIN2` on both columns. The primary key is (`Tag`, `StreamId`). A tag page reads that key from the last stream id. A trigger on `Streams` updates `StreamTags` when a row is inserted, when its tags or stream id change, and when the row is deleted. The trigger matches the stored stream id exactly, using `Latin1_General_100_BIN2`, against the old row and the new row. A change of spelling removes the tag rows for the old spelling and writes them for the new one. The trigger runs in the save's transaction, including a save from an older build. The aggregate type index is (`AggregateType`, `StreamIdBin`).

`InitializeSchemaAsync` adds the column, the indexes, `StreamTags`, and the trigger when they are missing. It then copies tags from the JSON in `Streams.Tags`, in batches, after the schema lock is released. That copy is recorded on `StreamTags` and does not run again. Existing rows stay. The events stamp stays at format 5. Call it before listing on a database created by an earlier build of this format. Adding `StreamIdBin` rewrites the stream table, so run that call when other writers can wait. A second call leaves the objects in place.

Several processes starting together take one lock for those objects and commit them together. The tag copy uses a separate lock, so a startup does not fail while another startup is still copying. The events stamp is unchanged.

## Upgrading from 0.6.0-alpha.2

0.6.0-alpha.2 stamped the events table `LawnDart_SchemaFormat` 2, the
outbox `LawnDart_OutboxSchemaFormat` 1, and the inbox
`LawnDart_InboxSchemaFormat` 1. This process stamps events 5, outbox 2,
and inbox 2.

Call `InitializeSchemaAsync` on the SQL event store once on each database
before the new process reads. Run that call from a deploy or admin step.
Normal app startup does not run it. At startup, call `VerifySchemaAsync` on
the event store, the outbox writer, and the snapshot store. It reads the
stamp and the objects. It does not create or change them. See
[Schema initialization](../SCHEMA_INITIALIZATION.md).
Call `InitializeSchemaAsync` on the outbox writer when
`EnableOutbox` is on. Call `InitializeSqlInboxStoreAsync` when the inbox
is SQL Server. A second call on a table that is already current returns.

An events table stamped 2, 3, or 4 is left as it is. Format 5 is a different
layout: the clustered key is the global position, and the stream id text
lives only on `Streams`. Drop the events tables and call
`InitializeSchemaAsync` again. This process does not copy those rows.

`Events.EventId` is unique per tenant on SQL Server. The in-memory log still
rejects the same id on a second tenant. An append whose metadata `EventId`
is already stored for that tenant throws `EventAlreadyAppendedException`.
The stored row stays. This applies to an app that writes `EventId` into
metadata and reuses that value on a later append in the same tenant. Use a
new id for each new event. When the call is a retry of an append that
already landed, catch `EventAlreadyAppendedException`. One metadata object
copied onto several events in a single append keeps the id for the first
event. The rest of that batch get distinct ids. A later append of the first
id in that tenant still throws.

Outbox format 1 becomes 2 by adding nullable `ClaimedUntil`. Inbox format
1 becomes 2 by making `ProcessedAt` nullable and adding nullable
`ClaimedAt`.

The outbox and inbox stamps move forward only. There is no downgrade. An older process that
does not accept the new stamp throws and names drop and recreate.

A missing events stamp, or any events stamp other than 5, throws the same
way. A missing stamp, or any stamp other than 1 or 2, throws on the outbox
and on the inbox. Back up the database before you drop a table. This process
does not copy the rows.

The steps run as separate batches. Run the call again after a failure.
Work that is already in place is skipped.

Adding `ClaimedUntil` and `ClaimedAt` is a short schema change.

## Related

- [Package map](README.md)
- [Schema initialization](../SCHEMA_INITIALIZATION.md)
- [Backend selection](../BACKEND_SELECTION.md)
- [Outbox](../OUTBOX_PATTERN.md)
- [Production shape](../learning-path/08-production.md)
