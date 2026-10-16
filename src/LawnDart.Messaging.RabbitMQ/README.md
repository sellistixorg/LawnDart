# LawnDart.Messaging.RabbitMQ

RabbitMQ transport for `LawnDart.Messaging`.

## What this project provides

- `AddRabbitMqTransport(...)` registration
- One quorum queue per subscription, bound to each handled event type. Set `QueueLayout` to `EventType` for one queue per event type. The exchange still comes from `ITopicMapper`.
- One retry exchange and queue, and one dead-letter exchange and queue, per subscription. Optional topology setup at startup.

Pair with `LawnDart.Messaging` for reactors and processors.
Pair with `LawnDart.Messaging.SqlServer` when inbox dedup must survive a restart.
