namespace LawnDart.Projections.Lightweight.Debug;

/// <summary>
/// Configuration for the lightweight projection debug API.
/// </summary>
public sealed class ProjectionDebugApiOptions
{
    internal const string DefaultRoutePrefix = "/internal/projections/debug";

    /// <summary>
    /// Gets or sets the route prefix under which the debug API is mapped.
    /// </summary>
    public string RoutePrefix { get; set; } = DefaultRoutePrefix;

    /// <summary>
    /// Gets or sets a value indicating whether the debug API requires an authenticated user.
    /// </summary>
    public bool RequireAuthentication { get; set; } = true;

    /// <summary>
    /// Gets or sets optional authorization policies applied to all debug endpoints.
    /// </summary>
    public string[] AuthorizationPolicyNames { get; set; } = [];

    /// <summary>
    /// Gets or sets the default page size for instance-list responses.
    /// </summary>
    public int DefaultInstancePageSize { get; set; } = 50;
}
