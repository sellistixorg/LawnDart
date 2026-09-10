using LawnDart;
using LawnDart.EventStore;
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
        var provider = new DefaultMetadataProvider();
        var businessTime = new DateTime(2020, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        var commandMetadata = new CommandMetadata
        {
            UserId = "user123",
            TenantId = "tenant456",
            CorrelationId = "corr-123",
            TraceId = "0af7651916cd43dd8448eb211c80319c",
            SpanId = "b7ad6b7169203331",
            Timestamp = DateTime.UtcNow
        };
        var @event = new TestEvent(Guid.NewGuid(), businessTime);
        var baseMetadata = new EventMetadata();

        var enriched = provider.EnrichEventMetadata(baseMetadata, commandMetadata, @event);

        Assert.Equal("user123", enriched.UserId);
        Assert.Equal("tenant456", enriched.TenantId);
        Assert.Equal("corr-123", enriched.CorrelationId);
        Assert.Equal("corr-123", enriched.CausationId);
        Assert.Equal(@event.Id.ToString(), enriched.EventId);
        Assert.Equal(businessTime, enriched.Timestamp);
        Assert.Null(enriched.CommitTimestamp);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", enriched.TraceId);
        Assert.Equal("b7ad6b7169203331", enriched.SpanId);
        Assert.Equal("default-metadata-provider-tests.test-event", enriched.SchemaName);
        Assert.NotEqual(@event.GetType().FullName, enriched.SchemaName);
    }

    [Fact]
    public void EnrichEventMetadata_UsesEventTimestamp_NotUtcNow()
    {
        var provider = new DefaultMetadataProvider();
        var businessTime = new DateTime(2019, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var @event = new TestEvent(Guid.NewGuid(), businessTime);

        var enriched = provider.EnrichEventMetadata(
            new EventMetadata(),
            new CommandMetadata(),
            @event);

        Assert.Equal(businessTime, enriched.Timestamp);
        Assert.NotEqual(DateTime.UtcNow.Date, enriched.Timestamp.Date);
    }

    [EventTypeName("default-metadata-provider-tests.test-event")]
    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}


