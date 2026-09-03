namespace LawnDart.Projections.Lightweight.Debug;

/// <summary>
/// Root configuration for projection debug services, HTTP API, and optional built-in UX.
/// </summary>
public sealed class ProjectionDebugOptions
{
    /// <summary>
    /// Gets or sets HTTP API options for <c>MapProjectionDebugApi()</c>.
    /// </summary>
    public ProjectionDebugApiOptions Api { get; set; } = new();

    /// <summary>
    /// Gets or sets built-in UX options when <see cref="ProjectionDebugUxOptions.Enabled"/> is <see langword="true"/>.
    /// </summary>
    public ProjectionDebugUxOptions? Ux { get; set; }

    internal List<ProjectionDebugEndpointRegistration> Endpoints { get; } = [];

    /// <summary>
    /// Returns endpoint registrations that have the built-in UX enabled.
    /// </summary>
    public IEnumerable<ProjectionDebugEndpointRegistration> GetUxEnabledEndpoints()
        => Endpoints.Where(e => e.Ux is { Enabled: true });
}
