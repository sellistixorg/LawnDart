---
name: lawndart-host-setup
description: Hub skill for wiring LawnDart in Program.cs — packages, AddLawnDart, AddBoundedContext order, and which specialized skill to open next.
---

# LawnDart host setup

Use this skill first when wiring a new application on LawnDart packages.

## Decision tree

1. **Tests only?** → `lawndart-event-store` → `UseInMemory()` on `"default"`.
2. **Durable / outbox?** → `lawndart-event-store` → `UseSqlServer()`.
3. **More than one domain store?** → `lawndart-bounded-context`.
4. **Read models?** → `lawndart-lightweight-projections` after the store.
5. **Domain types?** → `lawndart-domain-model`
6. **Projection classes?** → `lawndart-projection-authoring`
7. **HTTP?** → `lawndart-aspnet-hosting`

Bounded context ≠ multi-tenancy. Multiple contexts = separate event stores.

## Base registration

```csharp
builder.Services.AddLawnDart(opts =>
{
    opts.RequireTenantId = false;
    opts.EnableAuthorization = false;
});

builder.Services.AddInMemoryProjectionStores("default");
var ctx = builder.Services.AddBoundedContext("default");
ctx.UseInMemory();
ctx.WithCommandHandlers([typeof(CreateOrderHandler).Assembly]);
ctx.WithProjections([typeof(OrderSummaryProjection).Assembly]);
```

Zero-infra reference: `demos/LawnDart.Demo.Academy`.
