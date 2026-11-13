using LawnDart;
using LawnDart.EventStore;
using LawnDart.Publishing;

namespace LawnDart.EventSourcing.Testing;

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

[EventTypeName("backend-contract.translated-source")]
public sealed record ContractTranslatedSource : IEvent
{
    public Guid Id { get; init; }
    public DateTime Timestamp { get; init; }
    public string Value { get; init; } = "";
    public string InternalNote { get; init; } = "";
}

[EventTypeName("backend-contract.outbox-notice", version: 2)]
public sealed record ContractOutboxNotice(Guid Id, DateTime Timestamp, string Value) : IEvent;

[EventTypeName("backend-contract.internal-only")]
public sealed record ContractInternalEvent : IEvent
{
    public Guid Id { get; init; }
    public DateTime Timestamp { get; init; }
    public string Secret { get; init; } = "";
}

[EventTypeName("backend-contract.skipped-source")]
public sealed record ContractSkippedSource : IEvent
{
    public Guid Id { get; init; }
    public DateTime Timestamp { get; init; }
}

/// <summary>Declarations the outbox contract tests share.</summary>
public static class ContractPublication
{
    public static readonly Type[] SourceTypes =
    [
        typeof(ContractOutboxEvent),
        typeof(ContractTranslatedSource),
        typeof(ContractInternalEvent),
        typeof(ContractSkippedSource)
    ];

    public static void Declare(PublishedEventsBuilder builder)
    {
        builder
            .Publish<ContractOutboxEvent>()
            .Translate<ContractTranslatedSource, ContractOutboxNotice>(source =>
                new ContractOutboxNotice(source.Id, source.Timestamp, source.Value))
            .Translate<ContractSkippedSource, ContractOutboxNotice>(_ => null);
    }

    public static void DeclarePassThrough(PublishedEventsBuilder builder)
        => builder.Publish<ContractOutboxEvent>();
}

public static class ContractEvent
{
    public static ContractTestEvent Tick() => new(Guid.NewGuid(), DateTime.UtcNow);
}
