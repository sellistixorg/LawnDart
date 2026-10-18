# Stream IDs

A stream ID is the durable identity of an event sequence.

## Recommended shape

```
{tenantId}:{type}:{id}
```

Examples:

- `academy-tenant:Student:13bee0e1-9b78-4494-8570-0ad8608437d3`
- `default:CourseSection:{sectionId}`

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

Tenant prefix comes from `ITenantContextProvider`. See [Metadata](METADATA.md).
