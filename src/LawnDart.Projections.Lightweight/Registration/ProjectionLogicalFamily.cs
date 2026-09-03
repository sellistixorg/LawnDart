namespace LawnDart.Projections.Lightweight.Registration;

/// <summary>
/// Groups all discovered versions for a logical lightweight projection family.
/// </summary>
public sealed record ProjectionLogicalFamily
{
    /// <summary>
    /// Gets the logical family name.
    /// </summary>
    public required string LogicalName { get; init; }

    /// <summary>
    /// Gets the route-friendly kebab-case name derived from <see cref="LogicalName"/>.
    /// </summary>
    public required string KebabName { get; init; }

    /// <summary>
    /// Gets all registered versions for the family.
    /// </summary>
    public required IReadOnlyList<ProjectionRegistration> Versions { get; init; }

    /// <summary>
    /// Gets the resolved latest/default version for the family.
    /// </summary>
    public required ProjectionRegistration Latest { get; init; }

    /// <summary>
    /// Gets how the latest/default version was resolved.
    /// </summary>
    public required LatestVersionResolutionMode LatestResolutionMode { get; init; }
}
