# DI Grammar

Register event stores and Lightweight projections through a **named bounded context**.

```csharp
services.AddLawnDart(o => { o.RequireTenantId = false; });
services.AddInMemoryProjectionStores("default"); // before WithProjections (dev/test)

services.AddBoundedContext("default")
    .UseInMemory()
    // or .UseSqlServer(o => { o.ConnectionString = cs; })
    .WithCommandHandlers([typeof(MyHandler).Assembly])
    .WithProjections([typeof(MyProjection).Assembly]);

var app = builder.Build();
app.MapProjectionQueries("default");
```

| Piece | Role |
|---|---|
| `AddLawnDart` | Core options, default metadata provider |
| `AddBoundedContext(name)` | Starts a keyed context (`"default"` for single-store apps) |
| `UseInMemory` / `UseSqlServer` | Event store + repositories for that name. `UseInMemory` also registers `ICommandDispatcher` (`ContextAwareCommandDispatcher`). |
| `WithCommandHandlers` | Scans `ICommandHandler<T>` and registers `ICommandDispatcher`. The interface lives in Core (`LawnDart.Messaging` namespace); EventSourcing does not need the Messaging package to register it. |
| `WithProjections` | Lightweight projection runners (keyed) |
| `MapProjectionQueries(name)` | HTTP GETs for the keyed projection path |
| `AddLawnDartHttpCommands` / `MapLawnDartCommands` | HTTP POST command endpoints |

Academy is the copy-paste host: `demos/LawnDart.Demo.Academy` (console) and
`demos/LawnDart.Demo.Academy.WebApi`.

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs). The handler's `HandleAsync` loads the entity and calls the repository.
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization. Do not write a `HandleAsync<TCommand>` switch on the entity (obsolete).
3. **Load** with `GetOrCreateAsync<T>(id)` when the stream is `{type}:{guid}` (or `{tenant}:{type}:{guid}`). Use `GetAsync` / `CreateAsync` / `GetOrCreateAsync` with `string streamId` otherwise.
4. **Projections:** `ProjectionBase<TView>` plus attributes. Multi-stream views implement `IMultiStreamEntityResolver`.
5. **Stores:** `AddBoundedContext(name).UseInMemory()` / `UseSqlServer(...)`.
