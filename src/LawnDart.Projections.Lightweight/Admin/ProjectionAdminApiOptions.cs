namespace LawnDart.Projections.Lightweight.Admin;

/// <summary>
/// Configuration for the lightweight projection admin HTTP API
/// (<c>MapProjectionAdminApi</c>).
/// </summary>
public sealed class ProjectionAdminApiOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether rebuild endpoints require an authenticated user.
    /// Defaults to <see langword="true"/>.
    /// </summary>
    public bool RequireAuthentication { get; set; } = true;

    /// <summary>
    /// Gets or sets optional authorization policies applied to all rebuild endpoints.
    /// </summary>
    public string[] AuthorizationPolicyNames { get; set; } = [];

    /// <summary>
    /// When <see langword="true"/> (default), maps
    /// <c>POST /{context}/projections/{name}/rebuild</c> for each registered context
    /// that has a keyed <see cref="IProjectionAdmin"/>.
    /// </summary>
    public bool EnableRebuildEndpoints { get; set; } = true;
}
