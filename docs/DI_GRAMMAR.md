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
| `UseInMemory` / `UseSqlServer` | Event store + repositories for that name |
| `WithProjections` | Lightweight projection runners (keyed) |
| `MapProjectionQueries(name)` | HTTP GETs for the keyed projection path |
| `AddLawnDartHttpCommands` / `MapLawnDartCommands` | HTTP POST command endpoints |

Academy is the copy-paste host: `demos/LawnDart.Demo.Academy` (console) and
`demos/LawnDart.Demo.Academy.WebApi`.
