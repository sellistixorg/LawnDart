# LawnDart.Messaging.RabbitMQ

RabbitMQ transport for `LawnDart.Messaging`.

## What this project provides

- `AddRabbitMqTransport(...)` registration
- Direct exchanges named with the declared message type. Publish declares the exchange. Consumers declare the queue.
- A retry queue (default 5 seconds), a dead-letter exchange and queue, and optional topology setup at startup

Pair with `LawnDart.Messaging` for reactors and processors.
Pair with `LawnDart.Messaging.SqlServer` when inbox dedup must survive a restart.
