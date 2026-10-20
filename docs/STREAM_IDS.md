# Stream IDs

A stream ID is the durable identity of an event sequence.

## Recommended shape

```
{tenantId}:{type}:{id}
```

Examples:

- `academy-tenant:Student:13bee0e1-9b78-4494-8570-0ad8608437d3`
- `west:CourseSection:{sectionId}`

When `RequireTenantId` is false (Academy), still keep a stable prefix so IDs
stay unique across demos.

## Aggregates

One stream per aggregate instance. The repository loads that stream, applies
events to state, and appends with expected version.

`GetAsync(Guid)` / `GetOrCreateAsync(Guid)` / `CreateAsync(Guid)` still build
`{tenant}:{type}:{guid}` (or `{type}:{guid}` when no tenant). When the stream
is not that shape, for example
`{account}:InboundShipment:{plan}:{shipment}`, load with the string overloads:

```csharp
var streamId = $"{accountId}:InboundShipment:{planId:N}:{shipmentId}";
var shipment = await repo.GetOrCreateAsync<InboundShipmentAggregate>(streamId);
```

## DCB

DCB reads by tags. Appends still land in an event store stream. A tag
is a lowercase type, a colon, and the id (`student:{id}`). Build that
string with the context `ITagFormat`. Stream IDs stay
`{tenant}:{Type}:{id}`. See [TAGGING.md](TAGGING.md).

## HTTP handlers

Academy WebApi builds stream IDs from `ITenantContextProvider`:

```csharp
var tenantId = _tenant.GetTenantId() ?? "default";
var streamId = $"{tenantId}:Student:{command.StudentId}";
```

Tenant prefix comes from `ITenantContextProvider`. That host sets `RequireTenantId` to false, so the append does not run the tenant id check. With `RequireTenantId` on, `default` in any casing is rejected. See [Metadata](METADATA.md).

## Context names and tenant ids

A bounded context name starts with a letter or digit. Then letters, digits, `.`, `_`, or `-`. The longest name is 63 characters. `LawnDartNames.Pattern` is the expression. `LawnDartNames.IsValid` and `LawnDartNames.Validate` apply it.

The stored spelling is the spelling you registered. A second context whose name differs only by case is rejected. The default context is `default`. `Default` is rejected. Keyed services for the unkeyed alias use that lowercase spelling.

A tenant id uses the same character rule. `default` in any casing is reserved for a tenant id. `ValidateTenantId` rejects it when name validation is on. An empty string is rejected. A colon is rejected, because a stream id is `{tenantId}:{type}:{id}` and is split on every colon.

`LawnDartOptions.ValidateNames` defaults to true. Set it to false before `AddBoundedContext` when an existing name does not match. Call `AddLawnDart` first so registration sees the flag. The same flag keeps an existing tenant id of `default` in any casing. A colon in a tenant id is still rejected. An empty tenant id is still rejected.

`FixedTenantContextProvider` takes `validateNames` for the same flag. `AmbientTenantContextProvider` reads `LawnDartOptions` when dependency injection constructs it. `SetTenant` checks the id. An invalid `X-Tenant-Id` header returns 400. See [HTTP commands](HTTP_COMMANDS.md).

`HandleCommandAsync` and a SQL Server append check the tenant id when `RequireTenantId` is true. A missing tenant id throws. The in-memory save uses the same check. `UseSqlServer` copies `ValidateNames` onto `SqlServerEventStoreOptions.ValidateNames` before the SQL callback runs.

## Prefix match

`EnumerateStreamIdsAsync` and `GetStreamCountAsync` match a stream id prefix. `_`, `%`, and `[` in the prefix are literal characters. `acme_west:` does not match `acmeXwest:`.

The in-memory store compares with ordinal ignore-case. SQL Server uses the `StreamId` column collation, so `LIKE 'prefix%'` can seek the primary key. A new SQL Server database is case-insensitive. A case-sensitive database matches case.

## Length

SQL Server stores a stream id in `NVARCHAR(255)` on `Events`, `Streams`, and `EventSnapshots`. A longer id throws `ArgumentException` before the write.

`AddSqlProjectionStores` (package `LawnDart.Projections.Lightweight.SqlServer`) uses the context name as the SQL Server schema when you omit the schema. The name rule fits a schema identifier. Pass `schemaName: "dbo"` to keep the `dbo` schema.
