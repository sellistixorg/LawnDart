using LawnDart.Metadata;

namespace LawnDart.EventSourcing.Testing;

/// <summary>
/// Metadata for repository tests: a new correlation id per command and the tenant
/// from <see cref="ContractTenantContextProvider"/> when one is given.
/// </summary>
internal sealed class ContractMetadataProvider : IMetadataProvider
{
    private readonly ITenantContextProvider? _tenantProvider;

    public ContractMetadataProvider(ITenantContextProvider? tenantProvider = null)
        => _tenantProvider = tenantProvider;

    public CommandMetadata CaptureCommandMetadata(object? context = null)
        => new()
        {
            Timestamp = DateTime.UtcNow,
            CorrelationId = Guid.NewGuid().ToString(),
            TenantId = _tenantProvider?.GetTenantId(),
            UserId = "test-user"
        };

    public EventMetadata EnrichEventMetadata(
        EventMetadata eventMetadata,
        CommandMetadata commandMetadata,
        IEvent @event)
    {
        eventMetadata.CorrelationId = commandMetadata.CorrelationId;
        eventMetadata.TenantId = commandMetadata.TenantId;
        eventMetadata.UserId = commandMetadata.UserId;
        eventMetadata.CausationId = commandMetadata.CausationId ?? commandMetadata.CorrelationId;
        eventMetadata.EventId = @event.Id.ToString();
        eventMetadata.Timestamp = @event.Timestamp;
        eventMetadata.TraceId = commandMetadata.TraceId;
        eventMetadata.SpanId = commandMetadata.SpanId;
        return eventMetadata;
    }
}

/// <summary>A tenant provider that always returns one fixed tenant.</summary>
internal sealed class ContractTenantContextProvider : ITenantContextProvider
{
    private readonly string? _tenantId;

    public ContractTenantContextProvider(string? tenantId = "test-tenant")
        => _tenantId = tenantId;

    public string? GetTenantId() => _tenantId;

    public string GetTenantIdRequired()
        => _tenantId ?? throw new InvalidOperationException("Tenant ID is required but not available");
}
