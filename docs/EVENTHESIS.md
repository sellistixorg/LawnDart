# Eventhesis adapter

[Eventhesis](https://eventhesis.com) is one modelling tool that targets
LawnDart. It emits slice JSON. It does not generate LawnDart C# types.

The build kit is written against a **generic slice spec** (command, event,
entity, view, process, GWT, vertical slice). See
[skills/BUILD_KIT.md](https://github.com/sellistixorg/LawnDart/blob/main/skills/BUILD_KIT.md).

This page maps Eventhesis slice JSON onto that spec. A second canvas (prooph
board, eventmodelers.ai, and the like) is another adapter page.

Academy is a reference host, not generated output.

## Eventhesis concept to LawnDart

| Eventhesis | Generic spec | Typical LawnDart implementation |
|---|---|---|
| Command | Command | `ICommand` plus an aggregate or DCB handler. |
| Event | Event | `IEvent` (`Id` and `Timestamp` first if you use the Eventhesis field-order convention) plus `[EventTypeName("kebab-token")]`. Correlation, causation, and W3C trace live on the envelope (`EventMetadata` / `CommandMetadata`), not the payload. |
| Entity | Entity | `AggregateRoot<TState>` or `DcbEntity<TState>` declaring `Handle(TCommand)` |
| View | View | A read model. Lightweight: `ProjectionBase<TView>` plus scope attributes. Multi-stream views implement `IMultiStreamEntityResolver`. You may also fold against `IEventStore`. |
| Process / reaction | Process | `IReactor` / `IEventProcessor` / `ITaskProcessor` |
| GWT | GWT | `LawnDart.Testing` spec |
| Vertical slice | Vertical slice | `AddBoundedContext(name)` |

A View widget maps to a read model. The widget itself has no required CLR type.

Keep `ICommand` and `IEvent`. Hand-written events need a unique catalog token.
Put correlation, causation, and trace on the envelope, not the payload.

## Host grammar

See [DI Grammar](DI_GRAMMAR.md) for the frozen surface. A typical host:

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

Keep catalog tokens stable when you hand-write events. Additive JSON
keeps the same `SchemaVersion`. A breaking shape change keeps the token
and bumps the integer. See [Event schema versioning](EVENT_SCHEMA_VERSIONING.md).

Academy (`demos/LawnDart.Demo.Academy`) is a reference InMemory host with
optional SQL and HTTP commands via `MapLawnDartCommands`.
