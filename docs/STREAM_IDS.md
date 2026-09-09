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
is not that shape — for example
`{account}:InboundShipment:{plan}:{shipment}` — load with the string overloads:

```csharp
var streamId = $"{accountId}:InboundShipment:{planId:N}:{shipmentId}";
var shipment = await repo.GetOrCreateAsync<InboundShipmentAggregate>(streamId);
```

Do not call `SetStreamId` / `ReplayEvents` / `SetCommittedVersion` yourself.

## DCB

DCB reads by **tags**, not by guessing every stream. Appends still land in an
event store stream. Tags use `type:id`; stream IDs stay `{tenant}:{type}:{id}`.

## HTTP handlers

Academy WebApi builds stream IDs from `ITenantContextProvider`:

```csharp
var tenantId = _tenant.GetTenantId() ?? "default";
var streamId = $"{tenantId}:Student:{command.StudentId}";
```
