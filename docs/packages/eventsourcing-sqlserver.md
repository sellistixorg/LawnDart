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

A type filter reloads `EventTypes` when the token is missing from the
process cache. If the token is still absent, the read returns no rows and
`TypesResolved` is false. Do not checkpoint past that range. A projection
reads the range again on the next poll.

Init writes an extended property `LawnDart_SchemaFormat` on the events
table (current format `3`). Format `2` is upgraded in place.
`InitializeSchemaAsync` adds `Events.RowVersion` (`rowversion`), creates
`AppendFence`, and alters `EventTags.Tag` to `Latin1_General_100_BIN2`.
Existing rows stay. Call it once on each database before the new process
reads. A table with no stamp, or with any stamp other than `2` or `3`,
throws and names drop-and-recreate. There is no migration tool for those
stamps.

`RowVersion` is the commit horizon. An append inserts an `AppendFence` row
before it reserves a sequence, so the transaction owns a rowversion first.
Global reads and `GetCurrentSequenceAsync` include a row only when its
rowversion is below `MIN_ACTIVE_ROWVERSION()`. A sequence reserved by an
open append stays invisible until that append commits or rolls back. A
later committed sequence does not skip it. A transaction that writes any
`rowversion` in this database holds the horizon for every reader.
Read-only work does not.

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
whether or not an `IOutboxWriter` was injected. A row that exhausts
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
