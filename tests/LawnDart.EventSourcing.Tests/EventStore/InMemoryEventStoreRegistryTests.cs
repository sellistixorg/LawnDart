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
        var eventStore = new InMemoryEventStore(enableRegistry: true, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        var streamId = "Order:12345";
        var events = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Act
        await eventStore.AppendAsync(StreamId.Parse(streamId), events);
        var metadata = await eventStore.GetStreamAsync(StreamId.Parse(streamId));

        // Assert
        Assert.NotNull(metadata);
        Assert.Equal(streamId, metadata!.StreamId.ToString());
        Assert.Equal("Order", metadata.AggregateType);
        Assert.Equal(1, metadata.CurrentVersion);
        Assert.Equal(1, metadata.EventCount);
        Assert.Equal(StreamStatus.Active, metadata.Status);
    }

    [Fact]
    public async Task GetStreamsByAggregateTypeAsync_ReturnsMatchingStreams()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: true, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await eventStore.AppendAsync(StreamId.Parse("Order:12345"), new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });
        await eventStore.AppendAsync(StreamId.Parse("Order:67890"), new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });
        await eventStore.AppendAsync(StreamId.Parse("Customer:11111"), new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });

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
        var eventStore = new InMemoryEventStore(enableRegistry: true, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await eventStore.AppendAsync(StreamId.Parse("Order:12345"), [new TaggedEvent(new TestEvent(Guid.NewGuid(), DateTime.UtcNow), new[] { Tag.Parse("priority:high") })]);
        await eventStore.AppendAsync(StreamId.Parse("Order:67890"), [new TaggedEvent(new TestEvent(Guid.NewGuid(), DateTime.UtcNow), new[] { Tag.Parse("priority:low") })]);

        // Act
        var highPriorityStreams = await eventStore.GetStreamsByTagAsync(Tag.Parse("priority:high"));

        // Assert
        Assert.Single(highPriorityStreams);
        Assert.Contains(Tag.Parse("priority:high"), highPriorityStreams[0].Tags);
    }

    [Fact]
    public async Task EnumerateStreamIdsAsync_ReturnsAllStreamIds()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: true, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await eventStore.AppendAsync(StreamId.Parse("Order:12345"), new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });
        await eventStore.AppendAsync(StreamId.Parse("Order:67890"), new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });

        // Act
        var streamIds = await eventStore.EnumerateStreamIdsAsync();

        // Assert
        Assert.Equal(2, streamIds.Count);
        Assert.Contains("Order:12345", streamIds.Select(static s => s.ToString()).ToList());
        Assert.Contains("Order:67890", streamIds.Select(static s => s.ToString()).ToList());
    }

    [Fact]
    public async Task EnumerateStreamIdsAsync_WithPrefix_FiltersResults()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: true, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await eventStore.AppendAsync(StreamId.Parse("Order:12345"), new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });
        await eventStore.AppendAsync(StreamId.Parse("Customer:67890"), new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });

        // Act
        var orderStreamIds = await eventStore.EnumerateStreamIdsAsync("Order:");

        // Assert
        Assert.Single(orderStreamIds);
        Assert.Equal("Order:12345", orderStreamIds[0].ToString());
    }

    [Fact]
    public async Task GetStreamsUpdatedAfterAsync_ReturnsUpdatedStreams()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: true, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await eventStore.AppendAsync(StreamId.Parse("Order:12345"), new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });
        var firstPosition = (await eventStore.GetStreamAsync(StreamId.Parse("Order:12345")))!.LastSequencePosition;
        
        await eventStore.AppendAsync(StreamId.Parse("Order:67890"), new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) });

        // Act
        var updatedStreams = await eventStore.GetStreamsUpdatedAfterAsync(firstPosition);

        // Assert
        Assert.Single(updatedStreams);
        Assert.Equal("Order:67890", updatedStreams[0].StreamId.ToString());
    }

    [Fact]
    public async Task Registry_Disabled_ThrowsOnRegistryMethods()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: false, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => eventStore.GetStreamAsync(StreamId.Parse("Order:12345")));
    }
[EventTypeName("in-memory-event-store-registry-t.test-event")]

    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}


