# LawnDart.Messaging

Core messaging abstractions and hosted processing runtime.

## What this project provides

- Contracts (`IReactor<T>`, `IEventProcessor<T>`, `ITaskProcessor`, transport/inbox)
- `AddMessaging(...)` baseline registration
- Shared messaging options and processing primitives

Pair with `LawnDart.Messaging.InMemory` for zero-broker delivery.
