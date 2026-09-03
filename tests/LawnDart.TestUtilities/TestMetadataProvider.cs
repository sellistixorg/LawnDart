using LawnDart.Metadata;

namespace LawnDart.TestUtilities;

/// <summary>
/// Test implementation of IMetadataProvider that provides basic metadata.
/// </summary>
public class TestMetadataProvider : IMetadataProvider
{
    private readonly ITenantContextProvider? _tenantProvider;

    public TestMetadataProvider(ITenantContextProvider? tenantProvider = null)
    {
        _tenantProvider = tenantProvider;
    }

    public CommandMetadata CaptureCommandMetadata(object? context = null)
    {
        return new CommandMetadata
        {
            Timestamp = DateTime.UtcNow,
            CorrelationId = Guid.NewGuid().ToString(),
            TenantId = _tenantProvider?.GetTenantId(),
            UserId = "test-user"
        };
    }

    public EventMetadata EnrichEventMetadata(
        EventMetadata eventMetadata,
        CommandMetadata commandMetadata,
        IEvent @event)
    {
        eventMetadata.CorrelationId = commandMetadata.CorrelationId;
        eventMetadata.TenantId = commandMetadata.TenantId;
        eventMetadata.UserId = commandMetadata.UserId;
        eventMetadata.CausationId = commandMetadata.CorrelationId;
        eventMetadata.Timestamp = DateTime.UtcNow;
        
        return eventMetadata;
    }
}
