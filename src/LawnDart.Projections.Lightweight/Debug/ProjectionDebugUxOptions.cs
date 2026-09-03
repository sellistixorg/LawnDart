namespace LawnDart.Projections.Lightweight.Debug;

/// <summary>
/// Configuration for the built-in static projection debugger UX.
/// </summary>
public sealed class ProjectionDebugUxOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether the built-in UX is enabled.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Gets or sets the route at which the debugger page is served.
    /// </summary>
    public string Route { get; set; } = "/projectiondebugger";

    /// <summary>
    /// Gets or sets a value indicating whether the UX route requires an authenticated user.
    /// </summary>
    public bool RequireAuthentication { get; set; } = true;

    /// <summary>
    /// Gets or sets optional authorization policies applied to the UX route.
    /// Should match <see cref="ProjectionDebugApiOptions.AuthorizationPolicyNames"/> on the API.
    /// </summary>
    public string[] AuthorizationPolicyNames { get; set; } = [];

    /// <summary>
    /// Gets or sets the default page size when loading an instance timeline in the UX.
    /// </summary>
    public int DefaultTimelinePageSize { get; set; } = 500;
}
