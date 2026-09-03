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

## DCB

DCB reads by **tags**, not by guessing every stream. Appends still land in an
event store stream. Tags use `type:id`; stream IDs stay `{tenant}:{type}:{id}`.

## HTTP handlers

Academy WebApi builds stream IDs from `ITenantContextProvider`:

```csharp
var tenantId = _tenant.GetTenantId() ?? "default";
var streamId = $"{tenantId}:Student:{command.StudentId}";
```
