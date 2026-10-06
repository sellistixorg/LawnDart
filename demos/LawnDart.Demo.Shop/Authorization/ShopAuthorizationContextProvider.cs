using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using LawnDart.Authorization;
using LawnDart.Messaging;

namespace LawnDart.Demo.Shop.Authorization;

/// <summary>
/// Cookie claims for browser commands. Reactor and task dispatches have no HTTP user,
/// so they run as a demo system principal that can pay, ship, and cancel.
/// </summary>
public sealed class ShopAuthorizationContextProvider : IAuthorizationContextProvider
{
    private readonly IHttpContextAccessor _httpContextAccessor;

    /// <summary>
    /// Creates a provider that reads the current HTTP user, then the ambient message.
    /// </summary>
    public ShopAuthorizationContextProvider(IHttpContextAccessor httpContextAccessor)
        => _httpContextAccessor = httpContextAccessor;

    /// <inheritdoc />
    public bool CanProvideContext()
        => _httpContextAccessor.HttpContext?.User.Identity?.IsAuthenticated == true
           || AmbientMessageContext.Current is not null;

    /// <inheritdoc />
    public Task<AuthorizationContext?> GetAuthorizationContextAsync(CancellationToken cancellationToken = default)
    {
        // HTTP commands also push an ambient message. Only reactor and task dispatches
        // set shop-dispatch, so a buyer is not asked to ship their own order.
        var message = AmbientMessageContext.Current;
        if (message?.Headers.TryGetValue("shop-dispatch", out var dispatch) == true
            && dispatch == "background")
        {
            // DEMO ONLY: the background chain holds Seller and Warehouse so it can pay, ship, and cancel.
            return Task.FromResult<AuthorizationContext?>(new AuthorizationContext
            {
                UserId = message.UserId ?? "system",
                UserName = "system",
                UserRoles = ["Seller", "Warehouse"],
                TenantId = message.TenantId,
                TransportType = "Messaging",
                CorrelationId = message.CorrelationId
            });
        }

        var http = _httpContextAccessor.HttpContext;
        var user = http?.User;
        if (http is not null && user?.Identity?.IsAuthenticated == true)
        {
            return Task.FromResult<AuthorizationContext?>(new AuthorizationContext
            {
                UserId = user.FindFirst(ClaimTypes.NameIdentifier)?.Value,
                UserName = user.FindFirst(ClaimTypes.Name)?.Value,
                UserRoles = user.FindAll(ClaimTypes.Role).Select(c => c.Value).ToList(),
                UserClaims = user.Claims.Select(c => $"{c.Type}:{c.Value}").ToList(),
                TenantId = user.FindFirst("tenant_id")?.Value,
                TransportType = "HTTP",
                CorrelationId = http.TraceIdentifier
            });
        }

        return Task.FromResult<AuthorizationContext?>(null);
    }
}
