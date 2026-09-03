using Microsoft.Extensions.Logging;
using LawnDart.Authorization;

namespace LawnDart.Demo.Academy.WebApi.Authorization;

/// <summary>
/// Academy-specific authorization provider that extends the default claim-based checks
/// with a role-to-permission mapping tailored to the demo domain.
/// </summary>
/// <remarks>
/// <para>
/// In addition to the base-class behaviour (which grants access when the user carries a
/// <c>permission:{name}</c> claim or belongs to the <c>Admin</c> role), this provider
/// recognises two Academy-specific roles:
/// </para>
/// <list type="table">
///   <listheader><term>Role</term><description>Granted permissions</description></listheader>
///   <item>
///     <term><c>Instructor</c></term>
///     <description>
///       <see cref="AcademyPermissions.SectionCreate"/>,
///       <see cref="AcademyPermissions.SectionView"/>
///     </description>
///   </item>
///   <item>
///     <term><c>Student</c></term>
///     <description>
///       <see cref="AcademyPermissions.StudentView"/>,
///       <see cref="AcademyPermissions.StudentEnroll"/>,
///       <see cref="AcademyPermissions.SectionView"/>
///     </description>
///   </item>
/// </list>
/// <para>
/// Register this as a singleton to replace the default provider:
/// <code>
/// builder.Services.AddSingleton&lt;IAuthorizationProvider, AcademyAuthorizationProvider&gt;();
/// </code>
/// </para>
/// </remarks>
public sealed class AcademyAuthorizationProvider : DefaultAuthorizationProvider
{
    // Maps each role to the set of permissions it implicitly grants.
    private static readonly Dictionary<string, HashSet<string>> RolePermissions =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["Instructor"] = new(StringComparer.OrdinalIgnoreCase)
            {
                AcademyPermissions.SectionCreate,
                AcademyPermissions.SectionView
            },
            ["Student"] = new(StringComparer.OrdinalIgnoreCase)
            {
                AcademyPermissions.StudentView,
                AcademyPermissions.StudentEnroll,
                AcademyPermissions.SectionView,
                AcademyPermissions.EnrollmentIndexView
            }
        };

    /// <summary>
    /// Initializes a new <see cref="AcademyAuthorizationProvider"/>.
    /// </summary>
    /// <param name="logger">Optional logger.</param>
    public AcademyAuthorizationProvider(
        ILogger<AcademyAuthorizationProvider>? logger = null) : base(logger) { }

    /// <summary>
    /// Checks whether the user has the required permission, consulting both the base-class
    /// claim/Admin-role logic and the Academy role-to-permission mapping.
    /// </summary>
    /// <param name="context">The authorization context.</param>
    /// <param name="permission">The permission name to check.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// <see cref="AuthorizationResult.Success"/> when the check passes;
    /// <see cref="AuthorizationResult.Failure"/> otherwise.
    /// </returns>
    public override async Task<AuthorizationResult> CheckPermissionAsync(
        AuthorizationContext context,
        string permission,
        CancellationToken cancellationToken = default)
    {
        // Delegate to base first (handles explicit permission claims and Admin role)
        var baseResult = await base.CheckPermissionAsync(context, permission, cancellationToken);
        if (baseResult.IsAuthorized)
            return baseResult;

        // Academy role-to-permission expansion
        foreach (var role in context.UserRoles)
        {
            if (RolePermissions.TryGetValue(role, out var granted) &&
                granted.Contains(permission))
            {
                return AuthorizationResult.Success();
            }
        }

        return AuthorizationResult.Failure($"User lacks required permission: {permission}", permission);
    }
}
