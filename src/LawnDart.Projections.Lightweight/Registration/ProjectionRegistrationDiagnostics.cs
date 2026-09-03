using Microsoft.Extensions.Logging;

namespace LawnDart.Projections.Lightweight.Registration;

/// <summary>
/// Logging helpers for versioned lightweight projection registration metadata.
/// </summary>
internal static class ProjectionRegistrationDiagnostics
{
    internal static void LogLatestFallbackWarnings(
        ILogger? logger,
        ProjectionRegistrationCatalog catalog,
        string? contextName = null)
    {
        if (logger is null)
            return;

        foreach (var family in catalog.Families.Where(f => f.LatestResolutionMode == LatestVersionResolutionMode.HighestVersionFallback))
        {
            logger.LogWarning(
                "Projection family '{LogicalName}' {ContextMessage}did not declare IsLatest = true. " +
                "The latest/default alias will resolve to version v{Version} via highest-version fallback.",
                family.LogicalName,
                string.IsNullOrWhiteSpace(contextName) ? string.Empty : $"in context '{contextName}' ",
                family.Latest.Version);
        }
    }
}
