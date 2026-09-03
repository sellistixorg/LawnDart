using Microsoft.AspNetCore.Http;
using LawnDart.Authorization;
using System.Security.Claims;

namespace LawnDart.Authorization.AspNetCore;

/// <summary>
/// Captures authorization context from ASP.NET Core HttpContext.
/// </summary>
public class HttpAuthorizationContextProvider : IAuthorizationContextProvider
{
    private readonly IHttpContextAccessor _httpContextAccessor;
    
    /// <summary>
    /// Initializes a new instance of the HttpAuthorizationContextProvider.
    /// </summary>
    /// <param name="httpContextAccessor">HTTP context accessor.</param>
    public HttpAuthorizationContextProvider(IHttpContextAccessor httpContextAccessor)
    {
        _httpContextAccessor = httpContextAccessor ?? throw new ArgumentNullException(nameof(httpContextAccessor));
    }
    
    /// <summary>
    /// Returns true if HTTP context is available.
    /// </summary>
    public bool CanProvideContext()
    {
        return _httpContextAccessor.HttpContext != null;
    }
    
    /// <summary>
    /// Captures authorization context from HttpContext.User and headers.
    /// </summary>
    public Task<AuthorizationContext?> GetAuthorizationContextAsync(CancellationToken cancellationToken = default)
    {
        var httpContext = _httpContextAccessor.HttpContext;
        if (httpContext == null)
            return Task.FromResult<AuthorizationContext?>(null);
        
        var user = httpContext.User;
        var context = new AuthorizationContext
        {
            UserId = user.FindFirst(ClaimTypes.NameIdentifier)?.Value 
                    ?? user.FindFirst("sub")?.Value,
            UserName = user.FindFirst(ClaimTypes.Name)?.Value 
                      ?? user.FindFirst("name")?.Value,
            UserRoles = user.FindAll(ClaimTypes.Role).Select(c => c.Value).ToList(),
            UserClaims = user.Claims.Select(c => $"{c.Type}:{c.Value}").ToList(),
            TenantId = user.FindFirst("tenant_id")?.Value 
                      ?? httpContext.Request.Headers["X-Tenant-Id"].FirstOrDefault(),
            AccountId = user.FindFirst("account_id")?.Value,
            AccountEntitlements = user.FindAll("entitlement").Select(c => c.Value).ToList(),
            TransportType = "HTTP",
            CorrelationId = httpContext.TraceIdentifier
        };
        
        // Capture custom headers
        if (httpContext.Request.Headers.TryGetValue("X-Account-Id", out var accountId))
        {
            context.AccountId ??= accountId.FirstOrDefault();
        }
        
        return Task.FromResult<AuthorizationContext?>(context);
    }
}
