# Multi-context InMemory

Two bounded contexts, `ordering` and `catalog`, each with its own InMemory store in one host.

```bash
dotnet run --project demos/LawnDart.Demo.MultiContextInMemory
```

Requires the .NET 10 SDK. No Docker.

## What to look for

- Registered contexts are `ordering` and `catalog`. Multi-context mode is true.
- Ordering's global sequence is 3 after three appends. Catalog's global sequence is 2 after two appends.
- The two `IEventStore` instances are different objects.
- The first order stream reads back `OrderPlaced`, then `OrderFulfilled`.

Handlers inject `IEventStore` with no context key. `ContextAwareCommandDispatcher` routes each command through `ICommandContextRegistry`.

Both contexts live in this assembly. The host passes each context's event types and handler types explicitly (`WithEventTypes`, `WithCommandHandlers`).
