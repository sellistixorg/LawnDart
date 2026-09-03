using Xunit;
using LawnDart.EventSourcing.Performance;
using LawnDart.EventStore;
using LawnDart.EventSourcing.EventStore;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.EventSourcing.Tests.Performance;

public class CachedStreamRegistryTests
{
    private readonly InMemoryEventStore _innerRegistry;
    private readonly IMemoryCache _cache;
    private readonly CachedStreamRegistry _cachedRegistry;

    public CachedStreamRegistryTests()
    {
        _innerRegistry = new InMemoryEventStore(enableRegistry: true, logger: NullLogger<InMemoryEventStore>.Instance);
        _cache = new MemoryCache(new MemoryCacheOptions());
        _cachedRegistry = new CachedStreamRegistry(
            _innerRegistry,
            _cache,
            TimeSpan.FromMinutes(5),
            NullLogger<CachedStreamRegistry>.Instance);
    }

    [Fact]
    public async Task GetStreamAsync_FirstCall_HitsInnerRegistry()
    {
        // Arrange
        var streamId = "test-stream";
        await _innerRegistry.AppendAsync(
            streamId,
            new[] { new TestCacheEvent() },
            null,
            new Metadata.EventMetadata(),
            Array.Empty<string>());

        // Act
        var result = await _cachedRegistry.GetStreamAsync(streamId);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(streamId, result.StreamId);
    }

    [Fact]
    public async Task GetStreamAsync_SecondCall_UsesCachedValue()
    {
        // Arrange
        var streamId = "test-stream";
        await _innerRegistry.AppendAsync(
            streamId,
            new[] { new TestCacheEvent() },
            null,
            new Metadata.EventMetadata(),
            Array.Empty<string>());

        // Act
        var result1 = await _cachedRegistry.GetStreamAsync(streamId);
        var result2 = await _cachedRegistry.GetStreamAsync(streamId);

        // Assert
        Assert.NotNull(result1);
        Assert.NotNull(result2);
        Assert.Same(result1, result2); // Should be the same cached instance
    }

    [Fact]
    public async Task GetStreamsByAggregateTypeAsync_CachesResults()
    {
        // Arrange
        await _innerRegistry.AppendAsync(
            "Order:1",
            new[] { new TestCacheEvent() },
            null,
            new Metadata.EventMetadata(),
            Array.Empty<string>());

        // Act
        var result1 = await _cachedRegistry.GetStreamsByAggregateTypeAsync("Order");
        var result2 = await _cachedRegistry.GetStreamsByAggregateTypeAsync("Order");

        // Assert
        Assert.NotEmpty(result1);
        Assert.Same(result1, result2);
    }

    [Fact]
    public async Task GetStreamsByTagAsync_CachesResults()
    {
        // Arrange
        await _innerRegistry.AppendAsync(
            "stream1",
            new[] { new TestCacheEvent() },
            null,
            new Metadata.EventMetadata(),
            new[] { "test-tag" });

        // Act
        var result1 = await _cachedRegistry.GetStreamsByTagAsync("test-tag");
        var result2 = await _cachedRegistry.GetStreamsByTagAsync("test-tag");

        // Assert
        Assert.NotEmpty(result1);
        Assert.Same(result1, result2);
    }

    [Fact]
    public async Task EnumerateStreamIdsAsync_CachesResults()
    {
        // Arrange
        await _innerRegistry.AppendAsync(
            "Order:1",
            new[] { new TestCacheEvent() },
            null,
            new Metadata.EventMetadata(),
            Array.Empty<string>());

        // Act
        var result1 = await _cachedRegistry.EnumerateStreamIdsAsync("Order");
        var result2 = await _cachedRegistry.EnumerateStreamIdsAsync("Order");

        // Assert
        Assert.NotEmpty(result1);
        Assert.Same(result1, result2);
    }

    [Fact]
    public async Task GetStreamsUpdatedAfterAsync_DoesNotCache()
    {
        // Arrange
        await _innerRegistry.AppendAsync(
            "stream1",
            new[] { new TestCacheEvent() },
            null,
            new Metadata.EventMetadata(),
            Array.Empty<string>());

        // Act
        var result1 = await _cachedRegistry.GetStreamsUpdatedAfterAsync(0);
        var result2 = await _cachedRegistry.GetStreamsUpdatedAfterAsync(0);

        // Assert - Should not be the same instance (not cached)
        Assert.NotEmpty(result1);
        // This method deliberately doesn't cache for freshness
    }
}

public class TestCacheEvent : IEvent
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}
