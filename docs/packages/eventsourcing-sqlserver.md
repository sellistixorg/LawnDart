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
and append-condition fences use the `EventTags` side table (clustered on
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
A save from another process is still picked up on the poll interval. A
listener outside the store can raise the same signal.

A type filter reloads `EventTypes` when a name is missing from the
process cache. The current end is taken before that reload. A name that is
still absent matches no rows, and this read stops at that end. Events for
the other names are still returned. `TypesResolved` is false when any
requested name is still absent. Events up to the last one returned are
delivered. The position does not move past them to the current end until
every requested name is stored. A projection reads the range again on the
next poll.

Each appended record has an `EventId` (`UNIQUEIDENTIFIER`, unique). A typed
append stores `IEvent.Id`, including when several appends share one
`EventMetadata` object. A raw append uses metadata `EventId` when that
value is a GUID. Otherwise the store mints one before the commit and
reuses it if the commit is retried inside the same call. A second append
of the same id throws `EventAlreadyAppendedException`. The outbox row
uses that same id.

The store does not retry an append after `Commit` has been attempted. When
the commit reply is lost, it reads those event ids. If they are present, the
append result is the row that landed.

Init writes an extended property `LawnDart_SchemaFormat` on the events
table (current format `4`). Formats `2` and `3` are upgraded in place.
Format `4` adds a unique `Events.EventId`. Format `2` also gains the
format 3 objects below.
`InitializeSchemaAsync` adds `Events.RowVersion` (`rowversion`), creates
`AppendFence`, and alters `EventTags.Tag` to `Latin1_General_100_BIN2`.
Existing rows stay. Call it once on each database before the new process
reads. A table with no stamp, or with any stamp other than `2`, `3`, or `4`,
throws and names drop-and-recreate. Back up first. See
[Upgrading from 0.6.0-alpha.2](#upgrading-from-060-alpha2).

`RowVersion` is the commit horizon. An append inserts an `AppendFence` row
before it reserves a sequence, so the transaction owns a rowversion first.
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

Outbox rows copy the stored record in the same transaction: payload
bytes (`VARBINARY`), payload-format number `CodecId` (`TINYINT`), and `SchemaVersion` (`INT`),
none of them with a column default. Opening an older outbox table throws
and names drop-and-recreate. `EnableOutbox = true` writes those rows
whether or not an `IOutboxWriter` was injected. `OutboxClaimLease`
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

Optional `WithSnapshots` and outbox (`EnableOutbox`). Snapshot write
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

`InitializeSchemaAsync` adds the column, the indexes, `StreamTags`, and the trigger when they are missing. It then copies tags from the JSON in `Streams.Tags`, in batches, after the schema lock is released. That copy is recorded on `StreamTags` and does not run again. Existing rows stay. The events stamp stays at format 4. Call it before listing on a database created by an earlier build. Adding `StreamIdBin` rewrites the stream table, so run that call when other writers can wait. A second call leaves the objects in place.

Several processes starting together take one lock for those objects and commit them together. The tag copy uses a separate lock, so a startup does not fail while another startup is still copying. The events stamp is unchanged.

## Upgrading from 0.6.0-alpha.2

0.6.0-alpha.2 stamped the events table `LawnDart_SchemaFormat` 2, the
outbox `LawnDart_OutboxSchemaFormat` 1, and the inbox
`LawnDart_InboxSchemaFormat` 1. This process stamps events 4, outbox 2,
and inbox 2.

Call `InitializeSchemaAsync` on the SQL event store once on each database
before the new process reads. Run that call from a deploy or admin step.
Normal app startup does not run it. See
[Schema initialization](../SCHEMA_INITIALIZATION.md).
Call it on the outbox writer when
`EnableOutbox` is on. Call `InitializeSqlInboxStoreAsync` when the inbox
is SQL Server. A second call on a table that is already current returns.

A format 2 or 3 events table becomes 4 in that call. Format 3 is
`Events.RowVersion`, the `AppendFence` table, and `EventTags.Tag` set to
`Latin1_General_100_BIN2`. Format 4 adds `Events.EventId`
(`UNIQUEIDENTIFIER NOT NULL`) and a unique index. Existing rows are filled
with `NEWID()`. Those values are new. 0.6.0-alpha.2 did not store an event
id, so the filled value is not an old `IEvent.Id`.

`Events.EventId` is unique on SQL Server and in the in-memory log. An append
whose metadata `EventId` is already stored throws
`EventAlreadyAppendedException`. The stored row stays. This applies to an
app that writes `EventId` into metadata and reuses that value on a later
append. Use a new id for each new event. When the call is a retry of an
append that already landed, catch `EventAlreadyAppendedException`. One
metadata object copied onto several events in a single append keeps the id
for the first event. The rest of that batch get distinct ids. A later
append of the first id still throws.

A format 2 or 3 table has no `EventId` column, so that upgrade cannot
start from duplicate ids. If the column is already present and two rows
share a value, `CREATE UNIQUE INDEX UX_EventId` fails.
`InitializeSchemaAsync` throws that error and leaves the old stamp. Assign
distinct ids to the duplicates, then call it again.

Outbox format 1 becomes 2 by adding nullable `ClaimedUntil`. Inbox format
1 becomes 2 by making `ProcessedAt` nullable and adding nullable
`ClaimedAt`.

The stamp moves forward only. There is no downgrade. An older process that
does not accept the new stamp throws and names drop and recreate.

A missing stamp, or any stamp other than 2, 3, or 4 on events, 1 or 2 on
the outbox, and 1 or 2 on the inbox, throws the same way. Back up the
database before you drop the table. This process does not copy the rows.

The steps run as separate batches. Run the call again after a failure.
Work that is already in place is skipped.

On a large `Events` table, `UPDATE ... SET EventId = NEWID()` rewrites
every row that has no id. That holds locks and writes the log until it
finishes. Run it when other writers can wait. `ALTER TABLE` for
`RowVersion`, for `EventId` `NOT NULL`, and for the tag collation takes a
schema modification lock for that statement. The tag change drops and
rebuilds the `EventTags` primary key, so its duration follows the number
of tag rows. Adding `AppendFence`, `ClaimedUntil`, and `ClaimedAt` is a
short schema change.

## Related

- [Package map](README.md)
- [Schema initialization](../SCHEMA_INITIALIZATION.md)
- [Backend selection](../BACKEND_SELECTION.md)
- [Outbox](../OUTBOX_PATTERN.md)
- [Production shape](../learning-path/08-production.md)
