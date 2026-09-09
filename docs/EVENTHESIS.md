# Eventhesis and LawnDart

[Eventhesis](https://eventhesis.com) is a separate event-modelling canvas. It
emits a **slice-based JSON** event model for event-sourced implementation. It
does **not** generate LawnDart (or Patterns) C# types.

You implement the model however you choose. LawnDart is one runtime you can
target. Academy is a reference host, not generated output.

## Model → implementation (optional mapping)

| Eventhesis concept | Typical LawnDart implementation |
|---|---|
| Command | `ICommand` + aggregate / DCB handler |
| Event | `IEvent` (`Id`, `Timestamp` first if you use the Eventhesis field-order convention) |
| Entity | `AggregateRoot<TState>` or `DcbEntity<TState>` declaring `Handle(TCommand)` |
| View | A read model. Lightweight host: `ProjectionBase<TView>` + scope attributes + `IMultiStreamEntityResolver` when multi-stream (the Flywheel hook). Or your own projector against `IEventStore`. `IProjector` is experimental and unused — neither host calls it. |
| Process / reaction | `IReactor` / `IEventProcessor` / `ITaskProcessor` |
| GWT | `LawnDart.Testing` spec |
| Vertical slice | `AddBoundedContext(name)` |

There is no required CLR type for a View widget.

## Host grammar (if you use LawnDart)

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
var ctx = services.AddBoundedContext("default");
ctx.UseInMemory(); // or UseSqlServer(...)
ctx.WithProjections(...);
services.AddLawnDartHttpCommands(typeof(SomeCommand).Assembly);
app.MapLawnDartCommands();
```

Keep type names stable when you hand-write LawnDart code.
Rename only the package and extension-method prefixes (`AddLawnDart`,
`MapLawnDartCommands`, `AddLawnDartAuthorization`).

Academy (`demos/LawnDart.Demo.Academy`) is a reference InMemory host with
optional SQL and HTTP commands via `MapLawnDartCommands`.
