using System.Diagnostics;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Metadata;

namespace LawnDart;

/// <summary>
/// Default metadata provider that creates basic metadata.
/// Automatically populates TenantId from ITenantContextProvider if available.
/// </summary>
public class DefaultMetadataProvider : IMetadataProvider
{
    private readonly ITenantContextProvider? _tenantContextProvider;
    private readonly IEventTypeCatalog? _catalog;

    /// <summary>
    /// Initializes a new instance of DefaultMetadataProvider.
    /// </summary>
    public DefaultMetadataProvider()
        : this(tenantContextProvider: null, catalog: null)
    {
    }

    /// <summary>
    /// Initializes a new instance of DefaultMetadataProvider with tenant context provider.
    /// </summary>
    /// <param name="tenantContextProvider">Optional tenant context provider for auto-populating tenant ID.</param>
    public DefaultMetadataProvider(ITenantContextProvider? tenantContextProvider)
        : this(tenantContextProvider, catalog: null)
    {
    }

    /// <summary>
    /// Initializes a new instance of DefaultMetadataProvider with tenant context
    /// and the scoped event-type catalog used for <see cref="EventMetadata.SchemaName"/>.
    /// </summary>
    public DefaultMetadataProvider(
        ITenantContextProvider? tenantContextProvider,
        IEventTypeCatalog? catalog)
    {
        _tenantContextProvider = tenantContextProvider;
        _catalog = catalog;
    }

    /// <inheritdoc />
    public CommandMetadata CaptureCommandMetadata(object? context = null)
    {
        var message = context as MessageContext ?? AmbientMessageContext.Current;
        TryReadTrace(message, out var traceId, out var spanId);

        var correlationId = message?.CorrelationId ?? traceId ?? Guid.NewGuid().ToString();

        return new CommandMetadata
        {
            Timestamp = DateTime.UtcNow,
            CorrelationId = correlationId,
            CausationId = message?.CausationId,
            TenantId = message?.TenantId ?? _tenantContextProvider?.GetTenantId(),
            UserId = message?.UserId,
            TraceId = traceId,
            SpanId = spanId
        };
    }

    private static void TryReadTrace(MessageContext? message, out string? traceId, out string? spanId)
    {
        var activity = Activity.Current;
        if (activity is { IdFormat: ActivityIdFormat.W3C } && activity.TraceId != default)
        {
            traceId = activity.TraceId.ToHexString();
            spanId = activity.SpanId != default ? activity.SpanId.ToHexString() : null;
            return;
        }

        if (W3CTraceParent.TryGetIds(message?.Headers, out traceId, out spanId))
            return;

        traceId = null;
        spanId = null;
    }

    /// <inheritdoc />
    public EventMetadata EnrichEventMetadata(
        EventMetadata baseMetadata,
        CommandMetadata commandMetadata,
        IEvent @event)
    {
        return new EventMetadata
        {
            // Inherit from command
            UserId = commandMetadata.UserId,
            UserName = commandMetadata.UserName,
            TenantId = commandMetadata.TenantId,
            AccountId = commandMetadata.AccountId,
            IpAddress = commandMetadata.IpAddress,
            UserAgent = commandMetadata.UserAgent,
            CorrelationId = commandMetadata.CorrelationId,
            CausationId = commandMetadata.CausationId ?? commandMetadata.CorrelationId,
            AuthorizedBy = commandMetadata.AuthorizedBy,
            AuthorizedAt = commandMetadata.AuthorizedAt,
            AuthorizationPolicies = commandMetadata.AuthorizationPolicies,

            // Event-specific
            EventId = @event.Id.ToString(),
            Timestamp = @event.Timestamp,
            SchemaVersion = baseMetadata.SchemaVersion > 0 ? baseMetadata.SchemaVersion : 1,
            SchemaName = ResolveSchemaName(@event.GetType()),
            TraceId = commandMetadata.TraceId ?? baseMetadata.TraceId,
            SpanId = commandMetadata.SpanId ?? baseMetadata.SpanId,

            // Custom metadata
            Custom = commandMetadata.Custom
        };
    }

    private string ResolveSchemaName(Type eventType)
        => _catalog?.GetName(eventType)
           ?? EventTypeCatalog.TryGetDeclaredName(eventType)
           ?? throw new InvalidOperationException(
               $"Event type '{eventType.FullName}' must declare [EventTypeName(\"kebab-token\")].");
}


