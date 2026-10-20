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

    /// <summary>
    /// How long a live subscription waits before it reads again when no signal arrives.
    /// A save that commits in this process also wakes the subscription through
    /// <see cref="LawnDart.EventStore.ISignalsNewEvents"/>. Default: 1 second.
    /// </summary>
    public TimeSpan SubscriptionPollInterval { get; set; } = TimeSpan.FromSeconds(1);
}
