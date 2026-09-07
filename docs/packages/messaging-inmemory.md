# LawnDart.Messaging.InMemory

In-process `IMessageTransport`. Academy Showcase A uses it for choreography
without a broker.

Take it for local work and tests. There is no durable outbox relay on this
transport — SQL Server plus `EnableOutbox` is the durable publish path.

## Registration

```csharp
services.AddInMemoryMessaging();
```

That registers the transport and calls `AddMessaging()`. Then add reactors
or processors from `LawnDart.Messaging`.

## Related

- [Package map](README.md)
- [Messaging](messaging.md)
- [EDA patterns](../EDA-Patterns.md)
- [Outbox](../OUTBOX_PATTERN.md)
