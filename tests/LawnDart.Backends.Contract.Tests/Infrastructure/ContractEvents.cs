using LawnDart;
using LawnDart.EventStore;

namespace LawnDart.Backends.Contract.Tests;

[EventTypeName("backend-contract.tick")]
public sealed record ContractTick(Guid Id, DateTime Timestamp, string Name) : IEvent;

[EventTypeName("backend-contract.test-event")]
public sealed record ContractTestEvent(Guid Id, DateTime Timestamp) : IEvent;

[EventTypeName("backend-contract.another-event")]
public sealed record ContractAnotherEvent(Guid Id, DateTime Timestamp) : IEvent;

[EventTypeName("backend-contract.event-with-properties")]
public sealed record ContractEventWithProperties(Guid Id, DateTime Timestamp, string ProductId, int Quantity) : IEvent;

[EventTypeName("backend-contract.alias-event")]
public sealed record ContractAliasedEvent(Guid Id, DateTime Timestamp) : IEvent;

[EventTypeName("backend-contract.dcb-event")]
public sealed record ContractDcbEvent(Guid Id, DateTime Timestamp, int Value) : IEvent;

[EventTypeName("backend-contract.outbox-event")]
public sealed record ContractOutboxEvent : IEvent
{
    public Guid Id { get; init; }
    public DateTime Timestamp { get; init; }
    public string Value { get; init; } = "";
}

public static class ContractEvent
{
    public static ContractTestEvent Tick() => new(Guid.NewGuid(), DateTime.UtcNow);
}
