# DI Grammar

Register event stores and Lightweight projections through a **named bounded context**.

```csharp
services.AddLawnDart(o => { o.RequireTenantId = false; });
services.AddInMemoryProjectionStores("default"); // before WithProjections (dev/test)

var ctx = services.AddBoundedContext("default")
    .UseInMemory()
    // or .UseSqlServer(o => { o.ConnectionString = cs; })
    .WithCommandHandlers<MyHandler>()
    .WithEventTypes<MyEvent>()
    .WithUpcasters<MyUpcaster>() // when an event name has historical types
    .WithProjections([typeof(MyProjection).Assembly]);

services.AddLawnDartHttpCommands(typeof(MyHandler).Assembly);

var app = builder.Build();
app.MapLawnDartCommands();
app.MapProjectionQueries("default");
```

| Piece | Role |
|---|---|
| `AddLawnDart` | Core options, default metadata provider. Replace with `AddMetadataProvider<T>` ([Metadata](METADATA.md)). |
| `AddBoundedContext(name)` | Starts a keyed context (`"default"` for single-store apps) |
| `UseInMemory` / `UseSqlServer` | Event store + repositories for that name. `UseInMemory` also registers `ICommandDispatcher` (`ContextAwareCommandDispatcher`). |
| `WithCommandHandlers<TMarker>()` | The only handler registrar. Scans `ICommandHandler<T>` in the marker's assembly and registers `ICommandDispatcher`. The `"default"` context also gets an unkeyed handler alias built through `ContextServiceProvider`. |
| `WithEventTypes<TMarker>()` | Event catalog for that assembly. A scan that finds no `IEvent` types fails at warmup. |
| `WithUpcasters<TMarker>()` | Upcast hops for that assembly. Required when an event name has historical types. An incomplete chain throws at warmup. |
| `WithProjections` | Lightweight projection runners (keyed) |
| `MapProjectionQueries(name)` | HTTP GETs for the keyed projection path |
| `AddLawnDartHttpCommands` / `MapLawnDartCommands` | HTTP routing and authorization only. Does not register handlers. |

## Names

A context name matches `LawnDartNames.Pattern`. It starts with a letter or digit. Then letters, digits, `.`, `_`, or `-`. At most 63 characters. The spelling is kept. A second registration that differs only by case is rejected. `default` is the default context. `Default` is rejected, because the unkeyed alias uses that exact lowercase spelling.

`LawnDartOptions.ValidateNames` defaults to true. Set it to false before `AddBoundedContext` to keep a name that does not match. Call `AddLawnDart` first so the option is visible at registration. A colon in a tenant id is still rejected.

`AddSqlProjectionStores(contextName, connectionString)` uses `contextName` as the SQL Server schema when you pass no schema. Pass `schemaName: "dbo"` to keep the `dbo` schema. `AddInMemoryProjectionStores`, `AddTagProvider`, `AddTagFormat`, `MapProjectionQueries`, and `AddProjectionDebugServices` check the same name.

Academy is the copy-paste host: `demos/LawnDart.Demo.Academy` (console) and
`demos/LawnDart.Demo.Academy.WebApi`.

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs). The handler's `HandleAsync` loads the entity and calls the repository.
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization. The obsolete entity `HandleAsync<TCommand>` switch is not the authoring form.
3. **Load** with `GetOrCreateAsync<T>(id)` when the stream is `{type}:{guid}` (or `{tenant}:{type}:{guid}`). Use `GetAsync` / `CreateAsync` / `GetOrCreateAsync` with `string streamId` otherwise.
4. **Projections:** `ProjectionBase<TView>` plus attributes. Multi-stream views implement `IMultiStreamEntityResolver`.
5. **Stores:** `AddBoundedContext(name).UseInMemory()` / `UseSqlServer(...)`.
