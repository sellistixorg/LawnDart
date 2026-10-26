using LawnDart.EventStore;

namespace LawnDart.TestUtilities;

/// <summary>
/// Stand-in event type for tests that need <c>WithEventTypes</c> but do not
/// exercise typed hydrate of a domain event.
/// </summary>
[EventTypeName("test-utilities.catalog-placeholder")]
public sealed record CatalogPlaceholderEvent(Guid Id, DateTime Timestamp) : IEvent;

/// <summary>
/// The counter event from the quickstart. Schema startup samples register this type.
/// </summary>
[EventTypeName("counter-created")]
public sealed record CounterCreated(Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;
