using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using Xunit;

namespace LawnDart.EventSourcing.Tests.EventStore;

public class InMemoryEventStoreAdminTests
{
    private static IEvent[] OneEvent() => [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)];

    // ── GetCurrentSequenceAsync ───────────────────────────────────────────────

    [Fact]
    public async Task GetCurrentSequenceAsync_EmptyStore_ReturnsZero()
    {
        var store = new InMemoryEventStore();
        Assert.Equal(0L, await store.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task GetCurrentSequenceAsync_AfterSingleAppend_ReturnsOne()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("Order:1", OneEvent());
        Assert.Equal(1L, await store.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task GetCurrentSequenceAsync_AfterMultipleAppends_ReturnsLastSequence()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("Order:1", OneEvent());
        await store.AppendAsync("Order:2", OneEvent());
        await store.AppendAsync("Order:3", OneEvent());
        Assert.Equal(3L, await store.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task GetMaxSequencePositionAsync_EmptyStore_ReturnsZero()
    {
        var store = new InMemoryEventStore();
        Assert.Equal(0L, await store.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags("order:1"))));
        Assert.Equal(0L, await store.GetMaxSequencePositionAsync(Query.All()));
    }

    [Fact]
    public async Task GetMaxSequencePositionAsync_WithTag_ReturnsMaxMatchingSequence()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("Order:other", OneEvent(), tags: ["other"]);
        var first = await store.AppendAsync("Order:1", OneEvent(), tags: ["order:1"]);
        var last = await store.AppendAsync("Order:2", OneEvent(), tags: ["order:1"]);

        var query = Query.FromItems(QueryItem.ByTags("order:1"));
        Assert.Equal(last.SequencePositions[0], await store.GetMaxSequencePositionAsync(query));
        Assert.Equal(
            last.SequencePositions[0],
            await store.GetMaxSequencePositionAsync(query, fromSequencePosition: first.SequencePositions[0]));
        Assert.Equal(0L, await store.GetMaxSequencePositionAsync(query, fromSequencePosition: last.SequencePositions[0] + 1));
        Assert.Equal(await store.GetCurrentSequenceAsync(), await store.GetMaxSequencePositionAsync(Query.All()));
    }

    [Fact]
    public async Task GetMaxSequencePositionAsync_AndTags_ReturnsIntersectionMax()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("Order:1", OneEvent(), tags: ["a", "only-a"]);
        var both = await store.AppendAsync("Order:2", OneEvent(), tags: ["a", "b"]);
        await store.AppendAsync("Order:3", OneEvent(), tags: ["b"]);

        var max = await store.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags("a", "b")));
        Assert.Equal(both.SequencePositions[0], max);
    }

    // ── GetStreamCountAsync ───────────────────────────────────────────────────

    [Fact]
    public async Task GetStreamCountAsync_EmptyStore_ReturnsZero()
    {
        var store = new InMemoryEventStore(enableRegistry: true);
        Assert.Equal(0L, await store.GetStreamCountAsync());
    }

    [Fact]
    public async Task GetStreamCountAsync_AfterMultipleStreams_ReturnsCorrectCount()
    {
        var store = new InMemoryEventStore(enableRegistry: true);
        await store.AppendAsync("Order:1", OneEvent());
        await store.AppendAsync("Order:2", OneEvent());
        await store.AppendAsync("Customer:1", OneEvent());
        Assert.Equal(3L, await store.GetStreamCountAsync());
    }

    [Fact]
    public async Task GetStreamCountAsync_AppendingToSameStream_CountsOnce()
    {
        var store = new InMemoryEventStore(enableRegistry: true);
        await store.AppendAsync("Order:1", OneEvent());
        await store.AppendAsync("Order:1", OneEvent());
        Assert.Equal(1L, await store.GetStreamCountAsync());
    }

    [Fact]
    public async Task GetStreamCountAsync_WithPrefix_ReturnsFilteredCount()
    {
        var store = new InMemoryEventStore(enableRegistry: true);
        await store.AppendAsync("Order:1", OneEvent());
        await store.AppendAsync("Order:2", OneEvent());
        await store.AppendAsync("Customer:1", OneEvent());

        Assert.Equal(2L, await store.GetStreamCountAsync("Order:"));
        Assert.Equal(1L, await store.GetStreamCountAsync("Customer:"));
        Assert.Equal(0L, await store.GetStreamCountAsync("Product:"));
    }

    [Fact]
    public async Task GetStreamCountAsync_NullPrefix_ReturnsAllStreams()
    {
        var store = new InMemoryEventStore(enableRegistry: true);
        await store.AppendAsync("Order:1", OneEvent());
        await store.AppendAsync("Customer:1", OneEvent());

        Assert.Equal(2L, await store.GetStreamCountAsync(null));
    }

    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}
