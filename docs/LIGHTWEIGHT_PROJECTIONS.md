# Lightweight projections

`LawnDart.Projections.Lightweight` hosts in-process read models. Register
stores, then attach `ProjectionBase<TView>` types to a bounded context.

## Register

```csharp
services.AddInMemoryProjectionStores("default");
services.AddBoundedContext("default")
    .UseInMemory()
    .WithProjections(
        [typeof(StudentSummaryProjection).Assembly],
        opts =>
        {
            opts.PollInterval = TimeSpan.FromMilliseconds(200);
            opts.CheckpointInterval = 100;
        });

app.MapProjectionQueries("default");
```

Production-shaped stores:

```csharp
services.AddSqlProjectionStores("default", connectionString);
```

## Authoring

Extend `ProjectionBase<TView>` and decorate with a scope attribute
(`[SingleStreamProjection]`, `[GlobalProjection]`, `[DcbProjection]`,
`[MultiStreamProjection]`, or `[ProjectionEndpoint]`). `WithProjections`
scans those types.

Academy WebApi ships `StudentSummaryProjection`:

```csharp
[SingleStreamProjection("StudentSummary", streamType: "Student", tenantScope: TenantScope.TenantScoped)]
[ProjectionEndpoint(
    route: "/api/views/students/{studentId}",
    requiredPermission: AcademyPermissions.StudentView,
    cacheMaxAgeSeconds: 30)]
public class StudentSummaryProjection : ProjectionBase<StudentSummaryView>
{
    public void Handle(StudentRegistered e)
    {
        State.StudentId = e.StudentId;
        State.Name = e.Name;
        State.Email = e.Email;
        State.LastUpdated = e.Timestamp;
    }

    public void Handle(StudentEnrolled e)
    {
        State.ActiveEnrollments++;
        State.LastUpdated = e.Timestamp;
    }

    public void Handle(EnrollmentCancelled e)
    {
        State.ActiveEnrollments = Math.Max(0, State.ActiveEnrollments - 1);
        State.CancelledEnrollments++;
        State.LastUpdated = e.Timestamp;
    }
}
```

A view that spans stream types must also implement
`IMultiStreamEntityResolver` (`GetEntityId`). That is the multi-stream
hook.

Live poll, rebuild, and time-travel replay use the store's typed session.
Historical rows arrive as the current CLR type after upcast.

## Checkpoints

InMemory checkpoints reset when the process exits. SQL checkpoints resume
after restart.

## Poison events

When a compiled `Handle` method throws, the runner restores the in-memory
view to its pre-apply snapshot and retries 3 times (initial plus 2
retries). If every attempt fails:

- The global checkpoint does not advance past the failed sequence.
- Later events are not applied (halt is projection-wide on that node).
- The runner parks with `IsFaulted` until host stop or a projection rebuild.
- Recovery: fix the handler (or data), then rebuild or restart. Restart
  retries 3 times and halts again if the event still throws.

Unmatched event types, unowned partitions, and `GetEntityId == null`
still skip and advance the cursor.

## Debug / admin APIs

`AddProjectionDebugServices` and `MapProjectionAdminApi` are optional
local diagnostics.
