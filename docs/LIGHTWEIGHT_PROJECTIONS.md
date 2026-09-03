# Lightweight projections

`LawnDart.Projections.Lightweight` hosts in-process read models. Register
stores, then attach projectors to a bounded context.

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

A projector implements `IProjector` (or the Lightweight attributes / SDK types
in `LawnDart.Projections.Sdk`) and reduces events into a view. Academy WebApi
ships `StudentSummaryProjection`, section availability, and an enrollment index.

## Checkpoints

InMemory checkpoints reset when the process exits. SQL checkpoints resume after
restart.

## Poison events

When a compiled `Handle` method throws, the runner restores the in-memory view to
its pre-apply snapshot and retries **3** times (initial + 2 retries). If every
attempt fails:

- The global checkpoint does **not** advance past the failed sequence.
- Later events are not applied (halt is projection-wide on that node).
- The runner parks with `IsFaulted` until host stop or a projection rebuild.
- Recovery: fix the handler (or data), then rebuild or restart. Restart retries
  3 times and halts again if the event still throws.

Unmatched event types, unowned partitions, and `GetEntityId == null` still skip
and advance the cursor.

## Debug / admin APIs

`AddProjectionDebugServices` and `MapProjectionAdminApi` are optional local
diagnostics.
