namespace LawnDart.EventSourcing.EventStore;

/// <summary>
/// Options for <see cref="InMemoryEventStore"/>.
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

    /// <summary>
    /// Require a tenant id on append.
    /// When true, a stream id must include a tenant prefix, and a dynamic consistency boundary append must include <c>TenantId</c> in event metadata.
    /// When false, a tenant id is optional.
    /// </summary>
    /// <remarks>
    /// Default: false. <c>UseInMemory</c> copies <see cref="LawnDart.LawnDartOptions.RequireTenantId"/>
    /// onto this property when it creates the store. That host option defaults to true,
    /// so a hosted in-memory store checks tenant ids the same way a SQL Server append does.
    /// Set <see cref="ValidateNames"/> to false to keep an existing tenant id of <c>default</c>.
    /// An empty tenant id is still rejected. A colon in a tenant id is still rejected.
    /// </remarks>
    public bool RequireTenantId { get; set; } = false;

    /// <summary>
    /// Check tenant ids on append against <see cref="LawnDart.LawnDartNames.Pattern"/> when
    /// <see cref="RequireTenantId"/> is true.
    /// </summary>
    /// <remarks>
    /// Default: true. <c>UseInMemory</c> copies <see cref="LawnDart.LawnDartOptions.ValidateNames"/>
    /// onto this property when it creates the store.
    /// When this is false, a tenant id of <c>default</c> in any casing is allowed.
    /// An empty tenant id is still rejected. A colon in a tenant id is still rejected.
    /// </remarks>
    public bool ValidateNames { get; set; } = true;
}
