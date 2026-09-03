namespace LawnDart.EventSourcing.EventStore;

/// <summary>
/// Options for <see cref="InMemoryEventStore"/> portable subscriptions.
/// </summary>
public sealed class InMemoryEventStoreOptions
{
    /// <summary>
    /// Default per-handle bounded channel capacity for portable subscriptions.
    /// When full, the delivery loop blocks (no silent drop).
    /// </summary>
    public const int DefaultSubscriptionChannelCapacity = 10_000;

    /// <summary>
    /// Per-handle bounded channel capacity. Must be at least 1.
    /// </summary>
    public int SubscriptionChannelCapacity { get; set; } = DefaultSubscriptionChannelCapacity;
}
