namespace LawnDart.Projections.Lightweight.Debug;

/// <summary>
/// Per-endpoint projection debug configuration registered via
/// <c>AddProjectionDebugServices()</c>.
/// </summary>
public sealed class ProjectionDebugEndpointRegistration
{
    /// <summary>
    /// Key used to resolve keyed <see cref="ProjectionTimelineService"/> instances.
    /// <see langword="null"/> for the non-keyed single-context path.
    /// </summary>
    public string? DebugServiceKey { get; internal init; }

    internal string? PrimaryContextName { get; init; }

    internal string[] AdditionalContextNames { get; init; } = [];

    /// <summary>
    /// Gets HTTP API options for this debugger endpoint.
    /// </summary>
    public ProjectionDebugApiOptions Api { get; } = new();

    /// <summary>
    /// Gets built-in UX options for this debugger endpoint.
    /// </summary>
    public ProjectionDebugUxOptions? Ux { get; internal set; }
}
