using System.Text.Json;
using System.Text.Json.Serialization;
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

    [Fact]
    public void TraceIdAndSpanId_RoundTripOnMetadataJson()
    {
        var original = new EventMetadata
        {
            EventId = Guid.NewGuid().ToString(),
            Timestamp = new DateTime(2021, 3, 4, 5, 6, 7, DateTimeKind.Utc),
            TraceId = "0af7651916cd43dd8448eb211c80319c",
            SpanId = "b7ad6b7169203331",
            SchemaName = "demo.tick"
        };

        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        var json = JsonSerializer.Serialize(original, options);
        var restored = JsonSerializer.Deserialize<EventMetadata>(json, options);

        Assert.NotNull(restored);
        Assert.Equal(original.EventId, restored.EventId);
        Assert.Equal(original.Timestamp, restored.Timestamp);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", restored.TraceId);
        Assert.Equal("b7ad6b7169203331", restored.SpanId);
        Assert.Equal("demo.tick", restored.SchemaName);
        Assert.Contains("TraceId", json, StringComparison.Ordinal);
        Assert.Contains("SpanId", json, StringComparison.Ordinal);
    }
}


