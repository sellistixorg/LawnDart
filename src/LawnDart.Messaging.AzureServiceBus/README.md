# LawnDart.Messaging.AzureServiceBus

Azure Service Bus transport for `LawnDart.Messaging`.

## What this project provides

- `AddAzureServiceBusTransport(...)` registration
- Topics named with the declared message type. Publish creates the topic. Each subscriber creates one subscription.
- A retry delay (default 5 seconds). With dead-letter handling on, the last failure uses the subscription dead-letter subqueue.

Pair with `LawnDart.Messaging` for reactors and processors.
Pair with `LawnDart.Messaging.SqlServer` when inbox dedup must survive a restart.
