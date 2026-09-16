# LawnDart.EventSourcing.SqlServer

Durable SQL Server implementation of `IEventLog` plus the typed `IEventStore`
session. Same repositories and domain types as InMemory; only the backend
changes. The log stores recorded frames (family token, schema version,
content-type, payload bytes, metadata JSON). The session serializes on
append and hydrates on read.

Take it when you leave the zero-infra path. Keep the same bounded-context
name so projections and HTTP mapping stay pointed at the same stack.

## Payload layout

New appends write payload bytes to `EventPayload` (`VARBINARY(MAX)`). Reads
coalesce that column; when it is null, `EventData` (`NVARCHAR`) is treated as
legacy UTF-16 JSON and encoded to UTF-8. `SchemaVersion` is a first-class
`INT` (missing / null / 0 → `1`). Metadata stays `NVARCHAR` JSON.

SSMS: new `application/json` rows are UTF-8 in `VARBINARY`. Convert with
`CAST` / `CONVERT` when you want to read the JSON text; do not expect
`SELECT EventData` to be the write path.

```sql
SELECT CONVERT(varchar(max), EventPayload) COLLATE Latin1_General_100_CI_AS_SC_UTF8
FROM dbo.Events
WHERE ContentType = 'application/json';
```

Outbox rows copy the appended frame (family token, UTF-8 payload/metadata
text, `SchemaVersion`, `ContentType`) in the same append transaction. Old
rows without those columns read as version `1` and `application/json`.

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
