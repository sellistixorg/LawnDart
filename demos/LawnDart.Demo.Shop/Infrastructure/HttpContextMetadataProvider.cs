using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using LawnDart;
using LawnDart.Messaging;
using LawnDart.Metadata;

namespace LawnDart.Demo.Shop.Infrastructure;

/// <summary>
/// Reads command metadata from the signed-in browser user, then from the ambient
/// reactor message when a background dispatch has no HTTP user.
/// </summary>
public sealed class HttpContextMetadataProvider : IMetadataProvider
{
    private readonly IHttpContextAccessor _httpContextAccessor;

    /// <summary>
    /// Creates a provider that reads <see cref="HttpContext"/> and <see cref="AmbientMessageContext"/>.
    /// </summary>
    public HttpContextMetadataProvider(IHttpContextAccessor httpContextAccessor)
        => _httpContextAccessor = httpContextAccessor;

    /// <inheritdoc />
    public CommandMetadata CaptureCommandMetadata(object? context = null)
    {
        var http = _httpContextAccessor.HttpContext;
        var user = http?.User;
        if (http is not null && user?.Identity?.IsAuthenticated == true)
        {
            return new CommandMetadata
            {
                UserId = user.FindFirst(ClaimTypes.NameIdentifier)?.Value ?? "system",
                UserName = user.FindFirst(ClaimTypes.Name)?.Value ?? "system",
                TenantId = user.FindFirst("tenant_id")?.Value,
                CorrelationId = CorrelationScope.Current?.CorrelationId ?? http.TraceIdentifier,
                IpAddress = http.Connection.RemoteIpAddress?.ToString(),
                UserAgent = http.Request.Headers.UserAgent.ToString(),
                Timestamp = DateTime.UtcNow
            };
        }

        var message = context as MessageContext ?? AmbientMessageContext.Current;
        return new CommandMetadata
        {
            UserId = message?.UserId ?? "system",
            UserName = "system",
            TenantId = message?.TenantId,
            CorrelationId = CorrelationScope.Current?.CorrelationId
                ?? message?.CorrelationId
                ?? Guid.NewGuid().ToString(),
            CausationId = message?.CausationId,
            Timestamp = DateTime.UtcNow
        };
    }

    /// <inheritdoc />
    public EventMetadata EnrichEventMetadata(
        EventMetadata baseMetadata,
        CommandMetadata commandMetadata,
        IEvent @event)
    {
        return new EventMetadata
        {
            UserId = commandMetadata.UserId,
            UserName = commandMetadata.UserName,
            TenantId = commandMetadata.TenantId,
            CorrelationId = commandMetadata.CorrelationId,
            CausationId = commandMetadata.CausationId ?? commandMetadata.CorrelationId,
            IpAddress = commandMetadata.IpAddress,
            UserAgent = commandMetadata.UserAgent,
            EventId = @event.Id.ToString(),
            Timestamp = @event.Timestamp,
            SchemaVersion = baseMetadata.SchemaVersion > 0 ? baseMetadata.SchemaVersion : 1,
            SchemaName = @event.GetType().Name,
            TraceId = commandMetadata.TraceId,
            SpanId = commandMetadata.SpanId
        };
    }
}
