# LawnDart

Contracts and host core. Package id is `LawnDart`, not `LawnDart.Core`.
Every other package depends on this one.

Take it first. It does not include an event-store implementation.

## What you get

- `ICommand`, `IEvent`, `IState`
- `AggregateRoot<TState>`, `DcbEntity<TState>`, and their repository interfaces
- `IEventStore` — the store **abstraction** (implementations are in other packages)
- Authorization attributes and `AuthorizationService`
- `AddLawnDart`, `AddBoundedContext`, `AddLawnDartAuthorization`
- Nine pattern interfaces (`IReactor`, `IProjector`, `ITaskProcessor`, …)

## Registration

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default");
```

`AddBoundedContext` only starts the named context. Pair it with
`UseInMemory()` or `UseSqlServer(...)` from an EventSourcing package.

## Related

- [Package map](README.md)
- [DI Grammar](../DI_GRAMMAR.md)
- [Overview](../OVERVIEW.md)
