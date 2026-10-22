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

`HandleCommandAsync`, a SQL Server append, and an in-memory append check the tenant id when `RequireTenantId` is true. That option defaults to true on `LawnDartOptions`, `SqlServerEventStoreOptions`, and `InMemoryEventStoreOptions`, including a store constructed with `new InMemoryEventStore()`. A missing tenant id throws. `UseSqlServer` copies `ValidateNames` onto `SqlServerEventStoreOptions.ValidateNames` before the SQL callback runs. `UseInMemory` copies `RequireTenantId` and `ValidateNames` onto the in-memory store when it creates the store. Set `RequireTenantId` to false to append without a tenant id. An append whose stream id and event metadata name two different tenant ids throws. That check also runs when `RequireTenantId` is false.

## Stream pages

`ListStreamsAsync` returns one page of active streams for a network client or an admin tool. The page is ordered by stream id, character by character. Pass `NextPageToken` to the next call. A null token means this page is the last one.

`pageSize` is 1 to 1000. The default is 100. The call reads one extra row so the last page does not come back empty when the count divides evenly.

`StreamListFilter` can set a prefix, a tag, and an aggregate type. A stream must match every value that is set. The tenant id stays in the prefix, the same way it does for `EnumerateStreamIdsAsync` and `GetStreamCountAsync`.

A token is valid only on the store that issued it, and only with the same filter. The token is opaque base64url. The bytes are a version byte, a short hash of the filter, and the last stream id encoded as UTF-8. A token that cannot be read, or that belongs to a different filter, throws `ArgumentException`.

A stream created while you page is included when its id sorts after the last id already returned. Each stream id appears on at most one page. A stream that was stored before the page you just read, and that sorts after that page, is still available on a later page.

`GetStreamsByAggregateTypeAsync`, `GetStreamsByTagAsync`, and `EnumerateStreamIdsAsync` stay. Their order is undefined.

## Prefix match

`EnumerateStreamIdsAsync`, `GetStreamCountAsync`, and `ListStreamsAsync` match a stream id prefix. `_`, `%`, and `[` in the prefix are literal characters. `acme_west:` does not match `acmeXwest:`.

The in-memory store compares a prefix with ordinal ignore-case, and `ListStreamsAsync` orders the page character by character.

SQL Server matches a prefix with the `StreamId` column collation. A new database is case-insensitive. A case-sensitive database matches case. `ListStreamsAsync` still orders by `StreamIdBin` (`Latin1_General_100_BIN2`). A prefix with no letters is one range on that column: the page reads `StreamIdBin` from the later of the token and the prefix, and stops at the end of the range. A prefix that contains letters is not one range. The first page reads the clustered stream id key for that prefix, then orders those rows by `StreamIdBin`. A later page reads `StreamIdBin` from the token and applies the prefix. That later page has no end bound. A tag page reads `StreamTags` from the last stream id. Two stream ids that differ only by case are one stream on a case-insensitive database.

Postgres matches a prefix exactly, including case. When the prefix has three or more colon-separated parts, Postgres also limits the match to the tenant in the first part. A tag page reads `stream_tags` from the last stream id, then loads that stream row.

## Length

SQL Server stores a stream id in `NVARCHAR(255)` on `Events`, `Streams`, and `EventSnapshots`. A longer id throws `ArgumentException` before the write.

`AddSqlProjectionStores` (package `LawnDart.Projections.Lightweight.SqlServer`) uses the context name as the SQL Server schema when you omit the schema. The name rule fits a schema identifier. Pass `schemaName: "dbo"` to keep the `dbo` schema.
