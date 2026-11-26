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
    /// Controls how tenants share the store. Defaults to <see cref="LawnDart.Tenancy.Shared"/>.
    /// </summary>
    /// <remarks>
    /// <c>UseInMemory</c> copies <see cref="LawnDart.LawnDartOptions.Tenancy"/> onto this property.
    /// <see cref="LawnDart.Tenancy.Isolated"/> is not supported in this release and is refused at startup.
    /// </remarks>
    public Tenancy Tenancy { get; set; } = Tenancy.Shared;

    /// <summary>
    /// Optional fixed tenant id when <see cref="Tenancy"/> is <see cref="LawnDart.Tenancy.Single"/>.
    /// Leave null for no tenant. Must be null when <see cref="Tenancy"/> is <see cref="LawnDart.Tenancy.Shared"/>.
    /// </summary>
    public string? SingleTenantId { get; set; }

    /// <summary>
    /// Obsolete alias for <see cref="Tenancy"/>.
    /// <c>true</c> maps to <see cref="LawnDart.Tenancy.Shared"/>; <c>false</c> maps to <see cref="LawnDart.Tenancy.Single"/>.
    /// </summary>
    [Obsolete("Use Tenancy instead. RequireTenantId = true maps to Tenancy.Shared; false maps to Tenancy.Single.")]
    public bool RequireTenantId
    {
        get => Tenancy != Tenancy.Single;
        set => Tenancy = value ? Tenancy.Shared : Tenancy.Single;
    }

    /// <summary>
    /// Check tenant ids on append and query against <see cref="LawnDart.LawnDartNames.Pattern"/> when
    /// <see cref="Tenancy"/> is <see cref="LawnDart.Tenancy.Shared"/> or <see cref="SingleTenantId"/> is set.
    /// </summary>
    /// <remarks>
    /// Default: true. <c>UseInMemory</c> copies <see cref="LawnDart.LawnDartOptions.ValidateNames"/>
    /// onto this property when it creates the store.
    /// When this is false, a tenant id of <c>default</c> in any casing is allowed.
    /// An empty tenant id is still rejected. A colon in a tenant id is still rejected.
    /// </remarks>
    public bool ValidateNames { get; set; } = true;

    /// <summary>
    /// Validates tenancy, tenant name, subscription capacity, and poll interval.
    /// </summary>
    public void EnsureValid()
    {
        TenancyValidation.EnsureValid(Tenancy, SingleTenantId, ValidateNames);
        if (SubscriptionChannelCapacity < 1)
            throw new ArgumentOutOfRangeException(nameof(SubscriptionChannelCapacity), "SubscriptionChannelCapacity must be at least 1.");
        if (SubscriptionPollInterval < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(SubscriptionPollInterval), "SubscriptionPollInterval cannot be negative.");
    }
}
