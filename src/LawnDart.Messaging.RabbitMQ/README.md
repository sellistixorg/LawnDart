# LawnDart.Messaging.RabbitMQ

RabbitMQ transport for `LawnDart.Messaging`.

## What this project provides

- `AddRabbitMqTransport(...)` registration
- Direct exchanges named with the declared message type
- Retries, a dead-letter exchange and queue, and optional topology setup at startup

Pair with `LawnDart.Messaging` for reactors and processors.
Pair with `LawnDart.Messaging.SqlServer` when inbox dedup must survive a restart.
