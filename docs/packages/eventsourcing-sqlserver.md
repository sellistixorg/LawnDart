# LawnDart.EventSourcing.SqlServer

Durable SQL Server implementation of `IEventLog` plus the typed `IEventStore`
session. Same repositories and domain types as InMemory; only the backend
changes. The log stores recorded frames (family token, schema version,
codec id, payload bytes, metadata JSON). The session serializes on
append and hydrates on read.

Take it when you leave the zero-infra path. Keep the same bounded-context
name so projections and HTTP mapping stay pointed at the same stack.

## Payload layout

One payload column: `EventData VARBINARY(MAX) NOT NULL`. `SchemaVersion`
(`INT`), `CodecId` (`TINYINT`), and `EventTypeId` (`INT`) are `NOT NULL`
with no defaults. The session stamps all three on append. Family tokens
live in an `EventTypes` lookup (`Id INT IDENTITY`, `Token NVARCHAR(500)`
unique). `Events` stores `EventTypeId` and a foreign key; the store
caches token↔id both ways at schema init so reads do not join. A cache
miss reloads `EventTypes` once, then fails closed.

Type filters are `EventTypeId IN (...)`. A queried token that is not in
the cache matches zero rows and does not emit SQL. `IEventLog.AppendAsync`
still accepts an unregistered (foreign) token: get-or-insert runs in its
own short transaction and commits before the append transaction opens.
An orphan `EventTypes` row is harmless.

`Tags` is `NVARCHAR(4000)` and is a read projection only. DCB queries
and append-condition fences use the `EventTags` side table (clustered on
`(Tag, GlobalSequencePosition)`, collation `Latin1_General_100_BIN2`).
Tag equality is ordinal. `Customer:ABC` and `customer:abc` are different
tags. That table is always created and always written. There is no
`UseEventTagsTable` flag and no `OPENJSON` fallback on `Events.Tags`.
Metadata stays `NVARCHAR` JSON.

A subscription read asks for one page (`SqlSubscriptionBatchSize`) up to
the gap-free head. The scan cursor then moves to the highest position
examined, including positions that did not match the filter. A failed
read completes the subscription with `EventSubscriptionFailedException`.
Cancel and dispose still end it without an error.

A type filter reloads `EventTypes` when the token is missing from the
process cache. If the token is still absent, the read returns no rows and
`TypesResolved` is false. Do not checkpoint past that range. A projection
reads the range again on the next poll.

Each appended frame has an `EventId` (`UNIQUEIDENTIFIER`, unique). A typed
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
Format `3` gains `Events.EventId`. Format `2` also gains the format 3
objects below.
`InitializeSchemaAsync` adds `Events.RowVersion` (`rowversion`), creates
`AppendFence`, and alters `EventTags.Tag` to `Latin1_General_100_BIN2`.
Existing rows stay. Call it once on each database before the new process
reads. A table with no stamp, or with any stamp other than `2`, `3`, or `4`,
throws and names drop-and-recreate. There is no migration tool for those
stamps.

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
alongside the scalar columns and the family token from `EventTypes`.
Non-ASCII text needs a `_UTF8` collation on that cast (an operator
choice). The store does not detect server capability.

```sql
SELECT EventJson, EventType, EventTypeId, SchemaVersion, CodecId
FROM dbo.Events_Readable;
```

Outbox rows copy the appended frame in the same transaction: payload
bytes (`VARBINARY`), `CodecId` (`TINYINT`), and `SchemaVersion` (`INT`),
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
are tagged `Category=Integration`. The InMemory → SQL swap contract
(`Category=Contract`) covers dispatch, persist, reload, project, and
read-back. Outbox and subscriptions have their own tests. The same project also covers the
`IEventLog` contract (payload round-trip, token/tag query, thesis read
without CLR types). See
[BACKEND_SELECTION.md](../BACKEND_SELECTION.md).

InMemory remains the documented default for local work and Academy.

## Related

- [Package map](README.md)
- [Backend selection](../BACKEND_SELECTION.md)
- [Outbox](../OUTBOX_PATTERN.md)
- [Production shape](../learning-path/08-production.md)
