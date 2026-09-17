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
(`INT`) and `CodecId` (`TINYINT`) are `NOT NULL` with no defaults — the
session stamps both on append. `Tags` is `NVARCHAR(4000)` and is a read
projection only. Metadata stays `NVARCHAR` JSON.

Init writes an extended property `LawnDart_SchemaFormat` on the events
table. A table that is already present without that stamp, or with a
different value, throws. Drop and recreate; there is no in-place `ALTER`.

SSMS: `{table}_Readable` projects `CAST(EventData AS VARCHAR(MAX)) AS EventJson`
alongside the scalar columns. Non-ASCII text needs a `_UTF8` collation on
that cast — an operator choice; the store does not detect server capability.

```sql
SELECT EventJson, SchemaVersion, CodecId
FROM dbo.Events_Readable;
```

Outbox rows still copy the appended frame in the same transaction. The
outbox table layout is unchanged in this release.

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
read-back — not outbox or subscriptions. The same project also covers the
`IEventLog` contract (payload round-trip, token/tag query, thesis read
without CLR types). See
[BACKEND_SELECTION.md](../BACKEND_SELECTION.md).

InMemory remains the documented default for local work and Academy.

## Related

- [Package map](README.md)
- [Backend selection](../BACKEND_SELECTION.md)
- [Outbox](../OUTBOX_PATTERN.md)
- [Production shape](../learning-path/08-production.md)
