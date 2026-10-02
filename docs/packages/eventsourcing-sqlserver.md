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
`(Tag, GlobalSequencePosition)`). That table is always created and always
written. There is no `UseEventTagsTable` flag and no `OPENJSON` fallback
on `Events.Tags`. Metadata stays `NVARCHAR` JSON.

Init writes an extended property `LawnDart_SchemaFormat` on the events
table (current format `2`). Drop and recreate a table that is already
present without that stamp, or with a different value. Opening it throws
and names drop-and-recreate. There is no in-place `ALTER` and no
migration tool.

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
whether or not an `IOutboxWriter` was injected.

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
