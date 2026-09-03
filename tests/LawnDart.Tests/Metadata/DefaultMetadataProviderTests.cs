using LawnDart;
using LawnDart.Metadata;
using Xunit;

namespace LawnDart.Tests.Metadata;

public class DefaultMetadataProviderTests
{
    [Fact]
    public void CaptureCommandMetadata_ReturnsMetadataWithDefaults()
    {
        // Arrange
        var provider = new DefaultMetadataProvider();

        // Act
        var metadata = provider.CaptureCommandMetadata();

        // Assert
        Assert.NotNull(metadata);
        Assert.NotNull(metadata.CorrelationId);
        Assert.True(metadata.Timestamp <= DateTime.UtcNow);
    }

    [Fact]
    public void EnrichEventMetadata_InheritsFromCommandMetadata()
    {
        // Arrange
        var provider = new DefaultMetadataProvider();
        var commandMetadata = new CommandMetadata
        {
            UserId = "user123",
            TenantId = "tenant456",
            CorrelationId = "corr-123",
            Timestamp = DateTime.UtcNow
        };
        var @event = new TestEvent(Guid.NewGuid(), DateTime.UtcNow);
        var baseMetadata = new EventMetadata();

        // Act
        var enriched = provider.EnrichEventMetadata(baseMetadata, commandMetadata, @event);

        // Assert
        Assert.Equal("user123", enriched.UserId);
        Assert.Equal("tenant456", enriched.TenantId);
        Assert.Equal("corr-123", enriched.CorrelationId);
        Assert.Equal("corr-123", enriched.CausationId);
        Assert.Equal(@event.Id.ToString(), enriched.EventId);
        Assert.Equal(@event.GetType().FullName, enriched.SchemaName);
    }

    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}


