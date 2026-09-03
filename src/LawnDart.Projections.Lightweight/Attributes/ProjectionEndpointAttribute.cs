namespace LawnDart.Projections.Lightweight;

/// <summary>
/// Marks a projection class to have a <c>GET</c> API endpoint automatically registered
/// by <c>app.MapProjectionQueries()</c> for reading the projection's view state.
/// </summary>
/// <remarks>
/// <para>
/// When <c>app.MapProjectionQueries()</c> is called the framework scans all registered
/// projections for this attribute and registers a <c>GET</c> Minimal API endpoint at
/// <see cref="Route"/>.
/// </para>
/// <para>
/// The generated endpoint enforces authentication, an optional permission check via
/// <see cref="LawnDart.Authorization.IAuthorizationProvider"/>, and tenant
/// isolation derived from the projection's <c>TenantScope</c>. The tenant ID is always
/// read from the authenticated JWT — never from the request URL — to prevent
/// cross-tenant data access.
/// </para>
/// <example>
/// <code>
/// [SingleStreamProjection("StudentSummary", "Student")]
/// [ProjectionEndpoint(
///     route: "/api/views/students/{studentId}",
///     requiredPermission: AcademyPermissions.StudentView,
///     cacheMaxAgeSeconds: 30)]
/// public class StudentSummaryProjection : ProjectionBase&lt;StudentSummaryView&gt; { ... }
/// </code>
/// </example>
/// </remarks>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class ProjectionEndpointAttribute : Attribute
{
    /// <summary>
    /// Gets the route template for the generated GET endpoint
    /// (e.g., <c>"/api/views/students/{studentId}"</c>).
    /// </summary>
    public string Route { get; }

    /// <summary>
    /// Gets the optional permission name required to call this endpoint
    /// (e.g., <c>"Student.View"</c>). When <see langword="null"/> only authentication is required.
    /// </summary>
    public string? RequiredPermission { get; }

    /// <summary>
    /// Gets the maximum age in seconds for the <c>Cache-Control: max-age</c> response header.
    /// Zero or negative disables caching headers. Default is <c>0</c> (no cache directive added).
    /// </summary>
    public int CacheMaxAgeSeconds { get; }

    /// <summary>
    /// Initializes a new <see cref="ProjectionEndpointAttribute"/>.
    /// </summary>
    /// <param name="route">Route template for the generated GET endpoint.</param>
    /// <param name="requiredPermission">
    /// Optional permission name checked via
    /// <see cref="LawnDart.Authorization.IAuthorizationProvider.CheckPermissionAsync"/>.
    /// When <see langword="null"/> only authentication is enforced.
    /// </param>
    /// <param name="cacheMaxAgeSeconds">
    /// When positive, adds a <c>Cache-Control: max-age=N</c> header to successful responses.
    /// </param>
    public ProjectionEndpointAttribute(
        string route,
        string? requiredPermission = null,
        int cacheMaxAgeSeconds = 0)
    {
        Route = route ?? throw new ArgumentNullException(nameof(route));
        RequiredPermission = requiredPermission;
        CacheMaxAgeSeconds = cacheMaxAgeSeconds;
    }
}
