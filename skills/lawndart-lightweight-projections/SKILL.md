---
name: lawndart-lightweight-projections
description: Register LawnDart Lightweight projections — AddInMemoryProjectionStores or AddSqlProjectionStores, WithProjections, MapProjectionQueries.
---

# Lightweight projections

```csharp
services.AddInMemoryProjectionStores("default"); // or AddSqlProjectionStores(name, cs)

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

Call `Add*ProjectionStores` **before** `WithProjections`. Prefer the keyed
`MapProjectionQueries(name)` overload.

See `docs/LIGHTWEIGHT_PROJECTIONS.md`.
