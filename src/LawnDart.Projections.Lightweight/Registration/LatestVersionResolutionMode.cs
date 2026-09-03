namespace LawnDart.Projections.Lightweight.Registration;

/// <summary>
/// Describes how the latest/default version for a logical projection family was resolved.
/// </summary>
public enum LatestVersionResolutionMode
{
    /// <summary>
    /// The version was explicitly marked latest via the attribute metadata.
    /// </summary>
    Explicit,

    /// <summary>
    /// The version was selected because it is the only registered version in the family.
    /// </summary>
    SingleVersionImplicit,

    /// <summary>
    /// No version was explicitly marked latest, so the highest numeric version was selected.
    /// </summary>
    HighestVersionFallback
}
