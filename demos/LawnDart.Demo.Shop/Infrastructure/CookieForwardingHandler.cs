using Microsoft.AspNetCore.Http;

namespace LawnDart.Demo.Shop.Infrastructure;

/// <summary>
/// Delegating handler that copies the auth cookie from the current Blazor Server
/// circuit's HttpContext onto every outgoing self-hosted API request, so the
/// Minimal API endpoints see the authenticated user rather than anonymous.
/// </summary>
internal sealed class CookieForwardingHandler : DelegatingHandler
{
    private readonly IHttpContextAccessor _accessor;

    public CookieForwardingHandler(IHttpContextAccessor accessor)
    {
        _accessor = accessor;
    }

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var httpContext = _accessor.HttpContext;
        if (httpContext is not null)
        {
            var cookie = httpContext.Request.Headers.Cookie.ToString();
            if (!string.IsNullOrEmpty(cookie))
                request.Headers.TryAddWithoutValidation("Cookie", cookie);
        }

        return base.SendAsync(request, cancellationToken);
    }
}
