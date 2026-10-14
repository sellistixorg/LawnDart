# LawnDart.Messaging.Testing

Abstract xUnit conformance tests for an `IMessageTransport`.

Inherit `MessageTransportConformanceTests` from the transport's test project
and implement `CreateFixture`. The suite checks delivery, header pass-through,
the pre-handler hook, retries, dead-letter, cancellation, and a missing
`[EventTypeName]`.

See [Custom message transport](https://github.com/sellistixorg/LawnDart/blob/main/docs/CUSTOM_MESSAGE_TRANSPORT.md).
