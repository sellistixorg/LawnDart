namespace LawnDart;

/// <summary>
/// Cross-cutting configuration options for LawnDart.
/// Store-specific options live in their respective packages:
/// <c>SqlServerEventStoreOptions</c> (LawnDart.EventSourcing.SqlServer) for SQL Server event store,
/// <c>EventSourcingOptions</c> (LawnDart.EventSourcing) for in-process event sourcing.
/// </summary>
public class LawnDartOptions
{
    /// <summary>
    /// Require tenant ID in all framework operations.
    /// When true, TenantId must be present in CommandMetadata, and an in-memory append checks the tenant id.
    /// When false, tenant is optional.
    /// Default: true. <c>InMemoryEventStoreOptions.RequireTenantId</c> and <c>SqlServerEventStoreOptions.RequireTenantId</c> default to true as well.
    /// </summary>
    /// <remarks>
    /// <c>UseInMemory</c> copies this value onto the in-memory store when it creates the store.
    /// Set it to false to append without a tenant id.
    /// </remarks>
    public bool RequireTenantId { get; set; } = true;

    /// <summary>
    /// Check bounded context names and tenant ids against <see cref="LawnDartNames.Pattern"/>.
    /// </summary>
    /// <remarks>
    /// Default: true. Set false before <c>AddBoundedContext</c> when an existing name
    /// does not match the pattern, or to keep an existing tenant id of <c>default</c> in any casing.
    /// Call <c>AddLawnDart</c> first so registration sees the flag.
    /// An empty tenant id is still rejected. A colon in a tenant id is still rejected,
    /// because a stream id is split on every colon.
    /// <c>UseSqlServer</c> copies this value onto <c>SqlServerEventStoreOptions.ValidateNames</c>
    /// before the SQL callback runs.
    /// <c>UseInMemory</c> copies this value onto the in-memory store when it creates the store.
    /// </remarks>
    public bool ValidateNames { get; set; } = true;

    /// <summary>
    /// Enable authorization checks on command execution.
    /// When true, commands decorated with authorization attributes will be checked before execution.
    /// Requires AuthorizationService to be registered.
    /// Default: false
    /// </summary>
    public bool EnableAuthorization { get; set; } = false;
}
