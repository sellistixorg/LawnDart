# Overview

LawnDart is a .NET 10 library for the **nine common patterns** in the
command–event–state matrix. It is the in-process runtime that Eventhesis
compiles to.

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

Academy + `UseInMemory()` is the zero-infra path.
