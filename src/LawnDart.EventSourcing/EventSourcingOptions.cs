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
}
