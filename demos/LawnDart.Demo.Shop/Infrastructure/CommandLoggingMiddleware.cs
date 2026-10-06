using Microsoft.AspNetCore.Http;
using System.Security.Claims;

namespace LawnDart.Demo.Shop.Infrastructure;

/// <summary>
/// ASP.NET Core middleware that records every command dispatch into the CommandEventLog
/// for display in the Event Stream viewer. It hooks the /api/commands/* route.
/// </summary>
public sealed class CommandLoggingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly CommandEventLog _log;

    public CommandLoggingMiddleware(RequestDelegate next, CommandEventLog log)
    {
        _next = next;
        _log  = log;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        // Intercept all LawnDart command endpoints: /api/{group}/{action}
        var path = context.Request.Path;
        if (context.Request.Method == HttpMethods.Post &&
            path.StartsWithSegments("/api", out var remaining) &&
            !path.StartsWithSegments("/api/views"))
        {
            // Extract just the action portion (last path segment) as the command type label
            var commandType  = remaining.Value?.TrimStart('/') ?? "Unknown";
            var correlationId = CorrelationScope.Current?.CorrelationId ?? context.TraceIdentifier;
            var userId   = context.User.FindFirst(ClaimTypes.NameIdentifier)?.Value ?? "anonymous";
            var userName = context.User.FindFirst(ClaimTypes.Name)?.Value ?? "anonymous";

            _log.RecordCommand(
                commandId: Guid.NewGuid(),
                commandType: commandType,
                correlationId: correlationId,
                userId: userId,
                userName: userName,
                isReactorIssued: false);
        }

        await _next(context);
    }
}
