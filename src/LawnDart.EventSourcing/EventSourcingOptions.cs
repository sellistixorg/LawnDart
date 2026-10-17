namespace LawnDart.EventSourcing;

/// <summary>
/// Configuration options for event sourcing operations.
/// </summary>
public class EventSourcingOptions
{
    /// <summary>
    /// Enforce tenant isolation for DCB operations.
    /// When true, all DCB operations must include a tenant tag or have ITenantContextProvider available.
    /// When false, DCB operations can use any tags without tenant enforcement.
    /// Default: false
    /// </summary>
    public bool EnforceDcbTenantIsolation { get; set; } = false;

    /// <summary>
    /// How long a processor claim hides an outbox row from other processors.
    /// Default: 30 seconds.
    /// </summary>
    /// <remarks>
    /// SQL Server stores the deadline in <c>ClaimedUntil</c>, in whole seconds
    /// rounded up. The in-memory writer uses the full time span.
    /// <see cref="TimeSpan.Zero"/> expires the claim at once, so the next poll
    /// can take the row.
    /// </remarks>
    public TimeSpan OutboxClaimLease { get; set; } = TimeSpan.FromSeconds(30);
}
