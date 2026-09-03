using LawnDart.Metadata;
using Xunit;

namespace LawnDart.Tests.Metadata;

public class EventMetadataTests
{
    [Fact]
    public void Constructor_InitializesWithDefaultValues()
    {
        // Act
        var metadata = new EventMetadata();

        // Assert
        Assert.NotNull(metadata.Custom);
        Assert.NotNull(metadata.EventId);
        Assert.NotEqual(Guid.Empty, Guid.Parse(metadata.EventId));
        Assert.Equal(1, metadata.SchemaVersion);
        Assert.True(metadata.Timestamp <= DateTime.UtcNow);
    }

    [Fact]
    public void Properties_CanBeSet()
    {
        // Arrange
        var eventId = Guid.NewGuid().ToString();
        var metadata = new EventMetadata
        {
            EventId = eventId,
            UserId = "user123",
            TenantId = "tenant456",
            SchemaVersion = 2,
            SchemaName = "TestEvent",
            Timestamp = DateTime.UtcNow
        };

        // Assert
        Assert.Equal(eventId, metadata.EventId);
        Assert.Equal("user123", metadata.UserId);
        Assert.Equal("tenant456", metadata.TenantId);
        Assert.Equal(2, metadata.SchemaVersion);
        Assert.Equal("TestEvent", metadata.SchemaName);
    }
}


