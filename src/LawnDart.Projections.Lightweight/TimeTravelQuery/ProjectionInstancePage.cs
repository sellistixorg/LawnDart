namespace LawnDart.Projections.Lightweight.TimeTravelQuery;

/// <summary>
/// A paged list of projection view instance IDs filtered to a specific tenant.
/// </summary>
public sealed record ProjectionInstancePage
{
    /// <summary>The logical projection family name.</summary>
    public required string LogicalName { get; init; }

    /// <summary>The projection implementation version.</summary>
    public required int ProjectionVersion { get; init; }

    /// <summary>The internal storage key used for checkpoints and stored views.</summary>
    public required string ProjectionStorageKey { get; init; }

    /// <summary>Gets the compatibility alias for the logical projection family name.</summary>
    public string ProjectionName => LogicalName;

    /// <summary>Gets a value indicating whether this version owns the latest/default alias.</summary>
    public required bool IsLatest { get; init; }

    /// <summary>Gets how latest/default resolution was determined for the logical family.</summary>
    public required string LatestResolutionMode { get; init; }

    /// <summary>The tenant ID used to filter instances. Empty string for SystemGlobal projections.</summary>
    public required string TenantId { get; init; }

    /// <summary>The instance IDs visible to this tenant on the requested page.</summary>
    public required IReadOnlyList<string> Items { get; init; }

    /// <summary>The 0-based page number.</summary>
    public required int Page { get; init; }

    /// <summary>The requested maximum number of items per page.</summary>
    public required int PageSize { get; init; }

    /// <summary>The total number of instance IDs matching this tenant (across all pages).</summary>
    public required int TotalFiltered { get; init; }
}
