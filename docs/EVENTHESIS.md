# Eventhesis adapter

[Eventhesis](https://eventhesis.com) is one modelling tool that targets
LawnDart. It is not the reason the library exists.

The build kit is written against a **generic slice spec** (command, event,
entity, view, process, GWT, vertical slice). See
[skills/BUILD_KIT.md](../skills/BUILD_KIT.md).

This page is the **first modelling-tool adapter**. It maps Eventhesis slice
JSON onto that spec. Eventhesis does **not** generate LawnDart C# types. A
second canvas (prooph board, eventmodelers.ai, …) is another adapter page —
not a skill change.

Academy is a reference host, not generated output.

## Eventhesis concept → generic spec → LawnDart

| Eventhesis | Generic spec | Typical LawnDart implementation |
|---|---|---|
| Command | Command | `ICommand` + aggregate / DCB handler. There is no `IMessage` marker. |
| Event | Event | `IEvent` (`Id`, `Timestamp` first if you use the Eventhesis field-order convention) plus `[EventTypeName("kebab-token")]`. Correlation, causation, and W3C trace live on the envelope (`EventMetadata` / `CommandMetadata`), not the payload. |
| Entity | Entity | `AggregateRoot<TState>` or `DcbEntity<TState>` declaring `Handle(TCommand)` |
| View | View | A read model. Lightweight host: `ProjectionBase<TView>` + scope attributes + `IMultiStreamEntityResolver` when multi-stream. Or your own projector against `IEventStore`. `IProjector` is experimental and unused — neither host calls it. |
| Process / reaction | Process | `IReactor` / `IEventProcessor` / `ITaskProcessor` |
| GWT | GWT | `LawnDart.Testing` spec |
| Vertical slice | Vertical slice | `AddBoundedContext(name)` |

There is no required CLR type for a View widget. View mapping is unchanged.

Keep `ICommand` and `IEvent`. Hand-written events need a unique catalog token.
Do not put correlation / causation / trace on the payload.

## Host grammar (if you implement the slice on LawnDart)

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
var ctx = services.AddBoundedContext("default");
ctx.UseInMemory(); // or UseSqlServer(...)
ctx.WithEventTypes<SomeEvent>();
ctx.WithCommandHandlers<SomeHandler>();
ctx.WithProjections(...);
services.AddLawnDartHttpCommands(typeof(SomeCommand).Assembly);
app.MapLawnDartCommands();
```

<!-- TODO(RDY-10) -->

Keep catalog tokens stable when you hand-write events. Rename only the
package and extension-method prefixes (`AddLawnDart`,
`MapLawnDartCommands`, `AddLawnDartAuthorization`).

Academy (`demos/LawnDart.Demo.Academy`) is a reference InMemory host with
optional SQL and HTTP commands via `MapLawnDartCommands`.
