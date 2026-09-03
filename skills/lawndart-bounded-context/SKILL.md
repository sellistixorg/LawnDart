---
name: lawndart-bounded-context
description: Register named LawnDart bounded contexts, keyed stores, and WithCommandHandlers. Use when one process hosts more than one event store.
---

# Bounded context

```csharp
var orders = services.AddBoundedContext("orders");
orders.UseInMemory();
orders.WithCommandHandlers([typeof(PlaceOrderHandler).Assembly]);

var catalog = services.AddBoundedContext("catalog");
catalog.UseInMemory();
```

Single-store apps use name `"default"`. Keyed `IEventStore` /
`IAggregateRepository` / `IDcbRepository` resolve with that name.

For handlers that inject unkeyed `IEventStore`, bridge `"default"`:

```csharp
services.AddSingleton<IEventStore>(sp =>
    sp.GetRequiredKeyedService<IEventStore>("default"));
```

Do not treat contexts as tenants. Tenant IDs belong on stream IDs / metadata.
