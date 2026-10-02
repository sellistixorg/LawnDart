---
name: lawndart-lightweight-projections
description: Register LawnDart Lightweight projections. AddInMemoryProjectionStores or AddSqlProjectionStores, WithProjections, MapProjectionQueries. Use when hosting a read model.
---

# Lightweight projections

Author `ProjectionBase<TView>` plus a scope attribute. Host with
`WithProjections` after the event store. Call `Add*ProjectionStores`
**before** `WithProjections`. Prefer the keyed `MapProjectionQueries(name)`
overload.

Excerpt from `samples/Library.Host/LibraryHost.cs` (`AddInMemoryLibrary` / `MapLibraryHttp`):

```csharp
services.AddInMemoryProjectionStores("default");
```

```csharp
ctx.WithProjections(
    [typeof(LibraryCatalogProjection).Assembly],
    opts =>
    {
        opts.PollInterval = TimeSpan.FromMilliseconds(200);
        opts.CheckpointInterval = 100;
    });
```

```csharp
app.MapProjectionQueries("default");
```

`AddSqlProjectionStores(name, cs)` is the SQL store twin; the slice hosts InMemory.
After SQL stores, call `InitializeSqlProjectionStoresAsync` before runners start.

See `docs/LIGHTWEIGHT_PROJECTIONS.md`.
