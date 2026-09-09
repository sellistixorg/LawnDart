# Overview

LawnDart is a .NET 10 library for the **nine common patterns** in the
command–event–state matrix. Five cells are hosted; four are planned
interfaces with no host. [Eventhesis](https://eventhesis.com) is an optional
companion that emits slice JSON — it does not generate LawnDart types.

| From \ To | **Command** | **Event** | **State** |
|---|---|---|---|
| **Command** | Delegation 🔧 | Aggregate Root & DCB ✅ | Downstream Activity 🔧 |
| **Event** | Reaction ✅ | Event Processing ✅ | Projection ✅ |
| **State** | Task Processing ✅ | Event Generator 🔧 | State Transformation 🔧 |

✅ Hosted (runtime, DI, tests) · 🔧 Planned interface — `[Experimental]`, no host yet. Implementing a 🔧 type does not register or run it.

## Host grammar

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
var ctx = services.AddBoundedContext("default");
ctx.UseInMemory(); // or UseSqlServer(...)
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
