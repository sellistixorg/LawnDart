using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using LawnDart.Metadata;

namespace LawnDart.Projections.Lightweight.Security;

/// <summary>
/// Implements <see cref="ITenantContextProvider"/> by reading the tenant identifier from
/// the authenticated user's JWT claims inside an active <see cref="HttpContext"/>.
/// </summary>
/// <remarks>
/// <para>
/// The following claim types are checked in order:
/// <list type="number">
///   <item><c>tenant_id</c> — preferred claim name used by LawnDart-issued tokens.</item>
///   <item><c>tid</c> — Azure AD / Entra ID tenant identifier claim.</item>
/// </list>
/// </para>
/// <para>
/// Register this implementation instead of the default
 /// <see cref="LawnDart.Metadata.ITenantContextProvider"/>-based provider when
/// the application needs to derive the tenant from the inbound HTTP request:
/// </para>
/// <example>
/// <code>
/// builder.Services.AddScoped&lt;ITenantContextProvider, JwtTenantContextProvider&gt;();
/// </code>
/// </example>
/// </remarks>
public sealed class JwtTenantContextProvider : ITenantContextProvider
{
    private readonly IHttpContextAccessor _httpContextAccessor;

    /// <summary>
    /// Initializes a new <see cref="JwtTenantContextProvider"/>.
    /// </summary>
    /// <param name="httpContextAccessor">
    /// Accessor for the current <see cref="HttpContext"/>. Must be non-null.
    /// </param>
    public JwtTenantContextProvider(IHttpContextAccessor httpContextAccessor)
    {
        _httpContextAccessor = httpContextAccessor
            ?? throw new ArgumentNullException(nameof(httpContextAccessor));
    }

    /// <summary>
    /// Returns the tenant ID extracted from the current request's JWT, or
    /// <see langword="null"/> when there is no active <see cref="HttpContext"/> or
    /// neither expected claim is present.
    /// </summary>
    /// <returns>The tenant identifier string, or <see langword="null"/>.</returns>
    public string? GetTenantId()
    {
        var user = _httpContextAccessor.HttpContext?.User;
        if (user is null)
            return null;

        return user.FindFirstValue("tenant_id")
            ?? user.FindFirstValue("tid");
    }

    /// <summary>
    /// Returns the tenant ID extracted from the current request's JWT.
    /// </summary>
    /// <returns>The tenant identifier string.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when there is no active <see cref="HttpContext"/> or the tenant claim is absent.
    /// </exception>
    public string GetTenantIdRequired()
    {
        return GetTenantId()
            ?? throw new InvalidOperationException(
                "No tenant_id or tid claim found in the current JWT. " +
                "Ensure the request is authenticated and the token carries a tenant identifier.");
    }
}
