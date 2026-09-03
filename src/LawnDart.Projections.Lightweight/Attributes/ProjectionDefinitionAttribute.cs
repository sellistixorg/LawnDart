namespace LawnDart.Projections.Lightweight;

/// <summary>
/// Shared metadata for lightweight projection attributes.
/// Concrete projection-kind attributes inherit from this base so registration, routing,
/// and documentation can reason about a consistent identity/versioning contract.
/// </summary>
public abstract class ProjectionDefinitionAttribute : Attribute
{
    /// <summary>
    /// Initializes a new <see cref="ProjectionDefinitionAttribute"/>.
    /// </summary>
    /// <param name="logicalName">Logical family name exposed to callers and metadata.</param>
    /// <param name="tenantScope">Tenant isolation mode for the projection family.</param>
    protected ProjectionDefinitionAttribute(string logicalName, TenantScope tenantScope)
    {
        LogicalName = logicalName ?? throw new ArgumentNullException(nameof(logicalName));
        TenantScope = tenantScope;
    }

    /// <summary>
    /// Gets the logical projection family name, e.g. <c>"OrderSummary"</c>.
    /// Multiple concrete projection versions may share the same logical name.
    /// </summary>
    public string LogicalName { get; }

    /// <summary>
    /// Compatibility alias for the logical projection name.
    /// </summary>
    public string Name => LogicalName;

    /// <summary>
    /// Gets the tenant scope used when constructing view keys and securing query endpoints.
    /// </summary>
    public TenantScope TenantScope { get; }

    /// <summary>
    /// Gets or sets the projection implementation version. Defaults to <c>1</c>.
    /// </summary>
    public int Version { get; set; } = 1;

    /// <summary>
    /// Gets or sets a value indicating whether this version should own the unversioned/latest alias.
    /// When omitted across a projection family, the highest numeric version becomes latest automatically.
    /// </summary>
    public bool IsLatest { get; set; }

    /// <summary>
    /// Gets or sets the optional deprecation date as an ISO-8601 string
    /// (for example <c>"2026-06-01"</c>).
    /// </summary>
    public string? DeprecationDateIso { get; set; }
}
