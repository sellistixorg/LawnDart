using LawnDart.EventStore;

namespace LawnDart.Messaging.Testing;

/// <summary>
/// Message published by <see cref="MessageTransportConformanceTests"/>.
/// </summary>
[EventTypeName("messaging.conformance-ping")]
public sealed record ConformancePing(Guid Id, DateTime Timestamp, string Text) : IEvent;
