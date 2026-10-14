# LawnDart.Messaging

Core messaging abstractions and hosted processing runtime.

## What this project provides

- Contracts (`IReactor<T>`, `IEventProcessor<T>`, `ITaskProcessor`, transport/inbox)
- `AddMessaging(...)` baseline registration
- Shared messaging options, envelope headers, JSON settings, and processing primitives
- Startup check for declared message names and optional topology setup

Pair with `LawnDart.Messaging.InMemory` for zero-broker delivery.
Pair with `LawnDart.Messaging.RabbitMQ` for a RabbitMQ broker.
Pair with `LawnDart.Messaging.AzureServiceBus` for Azure Service Bus.
Pair with `LawnDart.Messaging.SqlServer` when inbox dedup must survive a restart.
