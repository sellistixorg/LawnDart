# LawnDart.Messaging.Testing

Abstract xUnit tests for an `IMessageTransport`.
Reference the package from the transport's test project and inherit
`MessageTransportConformanceTests`.

The suite checks delivery, user / tenant / custom header pass-through, the
pre-handler hook, retries up to the max attempts, dead-letter, cancellation,
and a missing `[EventTypeName]`. `InMemoryTransportConformanceTests` is the
in-process run of that suite. `RabbitMqTransportConformanceTests` runs it
against RabbitMQ.

## Registration

There is no host registration. Implement `CreateFixture` so each test gets a
transport, its `MessageTransportOptions`, and the dead-lettered messages.

```csharp
public sealed class MyTransportConformanceTests : MessageTransportConformanceTests
{
    protected override TransportConformanceFixture CreateFixture(
        Action<MessageTransportOptions>? configure = null,
        IMessagePreHandler? preHandler = null)
        => new MyFixture(configure, preHandler);
}
```

Guide: [Custom message transport](../CUSTOM_MESSAGE_TRANSPORT.md).

## Related

- [Package map](README.md)
- [Messaging](messaging.md)
- [InMemory messaging](messaging-inmemory.md)
