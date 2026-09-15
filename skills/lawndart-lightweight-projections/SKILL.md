---
name: lawndart-lightweight-projections
description: Register LawnDart Lightweight projections — AddInMemoryProjectionStores or AddSqlProjectionStores, WithProjections, MapProjectionQueries.
---

# Lightweight projections

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs).
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization.
3. **Load** by `string streamId` when the stream is not `{type}:{guid}`.
4. **Projections:** author `ProjectionBase<TView>` plus attributes; multi-stream views implement `IMultiStreamEntityResolver`. Host with `WithProjections`.
5. **Stores:** `UseInMemory` / `UseSqlServer` on `AddBoundedContext(name)` before `WithProjections`.

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

Call `Add*ProjectionStores` **before** `WithProjections`. Prefer the keyed
`MapProjectionQueries(name)` overload.

See `docs/LIGHTWEIGHT_PROJECTIONS.md`.
