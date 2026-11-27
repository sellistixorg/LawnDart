namespace LawnDart;

/// <summary>
/// Cross-cutting configuration options for LawnDart.
/// Store-specific options live in their respective packages:
/// <c>PostgresEventStoreOptions</c> (LawnDart.EventSourcing.Postgres) and
/// <c>SqlServerEventStoreOptions</c> (LawnDart.EventSourcing.SqlServer) for SQL stores,
/// <c>EventSourcingOptions</c> (LawnDart.EventSourcing) for in-process event sourcing.
/// </summary>
public class LawnDartOptions
{
    /// <summary>
    /// Controls how tenants share a bounded context's event store.
    /// Defaults to <see cref="LawnDart.Tenancy.Shared"/>.
    /// </summary>
    /// <remarks>
    /// <c>UseInMemory</c>, <c>UsePostgres</c>, and <c>UseSqlServer</c> copy this value onto the store options
    /// before store configuration runs.
    /// </remarks>
    public Tenancy Tenancy { get; set; } = Tenancy.Shared;

    /// <summary>
    /// Optional fixed tenant id when <see cref="Tenancy"/> is <see cref="LawnDart.Tenancy.Single"/>.
    /// Leave null when the store holds no tenant id. Must be null when <see cref="Tenancy"/> is <see cref="LawnDart.Tenancy.Shared"/>.
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
    /// Check bounded context names and tenant ids against <see cref="LawnDartNames.Pattern"/>.
    /// </summary>
    /// <remarks>
    /// Default: true. Set false before <c>AddBoundedContext</c> when an existing name
    /// does not match the pattern, or to keep an existing tenant id of <c>default</c> in any casing.
    /// Call <c>AddLawnDart</c> first so registration sees the flag.
    /// An empty tenant id is still rejected. A colon in a tenant id is still rejected,
    /// because a stream id is split on every colon.
    /// <c>UseSqlServer</c>, <c>UsePostgres</c>, and <c>UseInMemory</c> copy this value onto the store options.
    /// </remarks>
    public bool ValidateNames { get; set; } = true;

    /// <summary>
    /// Enable authorization checks on command execution.
    /// When true, commands decorated with authorization attributes will be checked before execution.
    /// Requires AuthorizationService to be registered.
    /// Default: false
    /// </summary>
    public bool EnableAuthorization { get; set; } = false;

    /// <summary>
    /// Validates tenancy and name settings.
    /// </summary>
    public void EnsureValid()
        => TenancyValidation.EnsureValid(Tenancy, SingleTenantId, ValidateNames);
}
