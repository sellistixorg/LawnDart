using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using Xunit;

namespace LawnDart.EventSourcing.Tests.EventStore;

public class InMemoryEventStoreRegistryTests
{
    [Fact]
    public async Task GetStreamAsync_AfterAppend_ReturnsMetadata()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: true);
        var streamId = "Order:12345";
        var events = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Act
        await eventStore.AppendAsync(streamId, events);
        var metadata = await eventStore.GetStreamAsync(streamId);

        // Assert
        Assert.NotNull(metadata);
        Assert.Equal(streamId, metadata!.StreamId);
        Assert.Equal("Order", metadata.AggregateType);
        Assert.Equal(1, metadata.CurrentVersion);
        Assert.Equal(1, metadata.EventCount);
        Assert.Equal(StreamStatus.Active, metadata.Status);
    }

    [Fact]
    public async Task GetStreamsByAggregateTypeAsync_ReturnsMatchingStreams()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: true);
        await eventStore.AppendAsync("Order:12345", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });
        await eventStore.AppendAsync("Order:67890", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });
        await eventStore.AppendAsync("Customer:11111", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });

        // Act
        var orderStreams = await eventStore.GetStreamsByAggregateTypeAsync("Order");

        // Assert
        Assert.Equal(2, orderStreams.Count);
        Assert.All(orderStreams, s => Assert.Equal("Order", s.AggregateType));
    }

    [Fact]
    public async Task GetStreamsByTagAsync_ReturnsMatchingStreams()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: true);
        await eventStore.AppendAsync("Order:12345", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, tags: new[] { "high-priority" });
        await eventStore.AppendAsync("Order:67890", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, tags: new[] { "low-priority" });

        // Act
        var highPriorityStreams = await eventStore.GetStreamsByTagAsync("high-priority");

        // Assert
        Assert.Single(highPriorityStreams);
        Assert.Contains("high-priority", highPriorityStreams[0].Tags);
    }

    [Fact]
    public async Task EnumerateStreamIdsAsync_ReturnsAllStreamIds()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: true);
        await eventStore.AppendAsync("Order:12345", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });
        await eventStore.AppendAsync("Order:67890", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });

        // Act
        var streamIds = await eventStore.EnumerateStreamIdsAsync();

        // Assert
        Assert.Equal(2, streamIds.Count);
        Assert.Contains("Order:12345", streamIds);
        Assert.Contains("Order:67890", streamIds);
    }

    [Fact]
    public async Task EnumerateStreamIdsAsync_WithPrefix_FiltersResults()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: true);
        await eventStore.AppendAsync("Order:12345", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });
        await eventStore.AppendAsync("Customer:67890", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });

        // Act
        var orderStreamIds = await eventStore.EnumerateStreamIdsAsync("Order:");

        // Assert
        Assert.Single(orderStreamIds);
        Assert.Equal("Order:12345", orderStreamIds[0]);
    }

    [Fact]
    public async Task GetStreamsUpdatedAfterAsync_ReturnsUpdatedStreams()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: true);
        await eventStore.AppendAsync("Order:12345", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });
        var firstPosition = (await eventStore.GetStreamAsync("Order:12345"))!.LastSequencePosition;
        
        await eventStore.AppendAsync("Order:67890", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });

        // Act
        var updatedStreams = await eventStore.GetStreamsUpdatedAfterAsync(firstPosition);

        // Assert
        Assert.Single(updatedStreams);
        Assert.Equal("Order:67890", updatedStreams[0].StreamId);
    }

    [Fact]
    public async Task Registry_Disabled_ThrowsOnRegistryMethods()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: false);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => eventStore.GetStreamAsync("Order:12345"));
    }

    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}


