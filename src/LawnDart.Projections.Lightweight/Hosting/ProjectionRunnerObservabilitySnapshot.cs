namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>Lightweight projection runtime fields exposed for <c>ITenantProjectionRuntimeMetrics</c>.</summary>
public readonly record struct ProjectionRunnerObservabilitySnapshot(
    string LogicalName,
    int Version,
    string StorageKey,
    long LastCheckpointSequence,
    double? MaxPipelineLagMs,
    bool IsFaulted,
    int WorkingSetCount = 0,
    long LastAppliedSequence = 0)
{
    /// <summary>
    /// Gets a display-friendly projection name that includes the version.
    /// </summary>
    public string ProjectionName => $"{LogicalName} v{Version}";
}
