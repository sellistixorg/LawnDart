# Eventhesis compile contract

Eventhesis widgets compile to LawnDart types and DI registrations.

| Eventhesis widget | LawnDart artifact |
|---|---|
| Command | `ICommand` + aggregate / DCB handler |
| Event | `IEvent` (`Id`, `Timestamp` first) |
| Entity (aggregate) | `AggregateRoot<TState>` or `DcbEntity<TState>` |
| View | `IProjector` + read model |
| Process / reaction | `IReactor` / `IEventProcessor` / `ITaskProcessor` |
| GWT | `LawnDart.Testing` spec |
| Vertical slice | `AddBoundedContext(name)` |

## Host grammar

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
var ctx = services.AddBoundedContext("default");
ctx.UseInMemory(); // or UseSqlServer(...)
ctx.WithProjections(...);
services.AddLawnDartHttpCommands(typeof(SomeCommand).Assembly);
app.MapLawnDartCommands();
```

Keep type names stable. Rename only the package and extension-method prefixes
(`AddLawnDart`, `MapLawnDartCommands`, `AddLawnDartAuthorization`).

Academy (`demos/LawnDart.Demo.Academy`) is the reference host Eventhesis
should be able to emit: InMemory by default, optional SQL, HTTP commands via
`MapLawnDartCommands`.
