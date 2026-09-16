# Overview

LawnDart is the .NET runtime that event-modeled systems compile into.

The shapes an event model compiles into — five hosted today — are on the
[CES matrix](CES_MATRIX.md). [Eventhesis](https://eventhesis.com) is one
modelling tool that targets LawnDart. It emits slice JSON — it does not
generate LawnDart types. See [Why LawnDart](WHY.md).

## Host grammar

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
var ctx = services.AddBoundedContext("default");
ctx.UseInMemory(); // or UseSqlServer(...)
ctx.WithCommandHandlers<SomeHandler>();
ctx.WithEventTypes<SomeEvent>();
ctx.WithProjections(...);
services.AddLawnDartHttpCommands(typeof(SomeCommand).Assembly);
app.MapLawnDartCommands();
```

## Packages (v1 happy path)

| Package | Role |
|---|---|
| `LawnDart` | Contracts: `ICommand`, `IEvent`, `IState`, aggregates, DCB, auth |
| `LawnDart.EventSourcing` | Runtime + InMemory store |
| `LawnDart.EventSourcing.SqlServer` | First durable store |
| `LawnDart.Messaging` | Reactors, processors, transports |
| `LawnDart.Messaging.InMemory` | In-process messaging |
| `LawnDart.Projections.Lightweight` | Read-model host (InMemory / SQL) |
| `LawnDart.AspNetCore` | HTTP command mapping |
| `LawnDart.Authorization.AspNetCore` | HTTP claims → `AuthorizationContext` |
| `LawnDart.Testing` | Given / when / then harnesses |

Academy + `UseInMemory()` is the zero-infra path. Per-package landing pages:
[Package map](packages/README.md).
