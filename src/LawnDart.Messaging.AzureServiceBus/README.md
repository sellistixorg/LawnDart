# LawnDart.Messaging.AzureServiceBus

Azure Service Bus transport for `LawnDart.Messaging`.

## What this project provides

- `AddAzureServiceBusTransport(...)` registration
- Topics named with the declared message type. Publish creates the topic. Each subscriber creates one subscription.
- A retry delay (default 5 seconds) that returns only to the subscription that failed. With dead-letter handling on, the last failure uses the subscription dead-letter subqueue.

## Routing

Publish sets the application property `lawndart_target` to `*`.
A retry sets it to the subscription that failed.
Each subscription's rule is:

```text
user.lawndart_target IS NULL OR user.lawndart_target = '*' OR user.lawndart_target = '<name>'
```

`<name>` is that subscription. A message with no `lawndart_target` reaches every subscription. Other publishers can omit the property. A retry reaches only the subscription named in the property.

Pair with `LawnDart.Messaging` for reactors and processors.
Pair with `LawnDart.Messaging.SqlServer` when inbox dedup must survive a restart.
