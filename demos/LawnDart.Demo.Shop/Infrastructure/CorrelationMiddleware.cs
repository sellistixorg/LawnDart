using Microsoft.AspNetCore.Http;
using System.Security.Claims;

namespace LawnDart.Demo.Shop.Infrastructure;

/// <summary>
/// ASP.NET Core middleware that establishes a CorrelationScope for every incoming HTTP request.
/// This scope propagates through the async call chain so command handlers, aggregates,
/// and the LoggingEventStore all see the same correlation context.
/// </summary>
public sealed class CorrelationMiddleware
{
    private readonly RequestDelegate _next;

    public CorrelationMiddleware(RequestDelegate next) => _next = next;

    public async Task InvokeAsync(HttpContext context)
    {
        var correlationId = context.TraceIdentifier;
        var userId   = context.User.FindFirst(ClaimTypes.NameIdentifier)?.Value ?? "anonymous";
        var userName = context.User.FindFirst(ClaimTypes.Name)?.Value ?? "anonymous";

        using var scope = CorrelationScope.Begin(new CorrelationContext(correlationId, userId, userName));
        await _next(context);
    }
}
