using LawnDart.EventStore;

namespace LawnDart.TestUtilities;

/// <summary>
/// Stand-in event type for tests that need <c>WithEventTypes</c> but do not
/// exercise typed hydrate of a domain event.
/// </summary>
[EventTypeName("test-utilities.catalog-placeholder")]
public sealed record CatalogPlaceholderEvent(Guid Id, DateTime Timestamp) : IEvent;
