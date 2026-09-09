# Step 4 — Reading state

**Previous:** [Testing](03-testing-your-aggregate.md) · **Next:** [DCB](05-dcb-patterns.md)

Projections turn the event log into views. You may hand-roll a projector or
use the shipped host. LawnDart ships `LawnDart.Projections.Lightweight` only.
Author `ProjectionBase<TView>` plus scope attributes. `IProjector` is
experimental and unused — this host does not call it. A view that spans
stream types implements `IMultiStreamEntityResolver` on the handler
(the Flywheel multi-stream hook).

```csharp
services.AddInMemoryProjectionStores("default");
services.AddBoundedContext("default")
    .UseInMemory()
    .WithProjections([typeof(StudentSummaryProjection).Assembly]);

app.MapProjectionQueries("default");
```

Author a projector that applies student/section events into a DTO. Academy
WebApi maps GET `/api/views/students/{studentId}`.

Details: [LIGHTWEIGHT_PROJECTIONS.md](../LIGHTWEIGHT_PROJECTIONS.md).
