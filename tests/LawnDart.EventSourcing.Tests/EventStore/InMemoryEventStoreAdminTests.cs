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
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        Assert.Equal(0L, await store.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task GetCurrentSequenceAsync_AfterSingleAppend_ReturnsOne()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await store.AppendAsync(StreamId.Parse("Order:1"), OneEvent());
        Assert.Equal(1L, await store.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task GetCurrentSequenceAsync_AfterMultipleAppends_ReturnsLastSequence()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await store.AppendAsync(StreamId.Parse("Order:1"), OneEvent());
        await store.AppendAsync(StreamId.Parse("Order:2"), OneEvent());
        await store.AppendAsync(StreamId.Parse("Order:3"), OneEvent());
        Assert.Equal(3L, await store.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task GetMaxSequencePositionAsync_EmptyStore_ReturnsZero()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        Assert.Equal(0L, await store.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags(Tag.Parse("order:1")))));
        Assert.Equal(0L, await store.GetMaxSequencePositionAsync(Query.All()));
    }

    [Fact]
    public async Task GetMaxSequencePositionAsync_WithTag_ReturnsMaxMatchingSequence()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await store.AppendAsync(StreamId.Parse("Order:other"), OneEvent().Select(e => new TaggedEvent(e, [Tag.Parse("tag:other")])).ToList());
        var first = await store.AppendAsync(StreamId.Parse("Order:1"), OneEvent().Select(e => new TaggedEvent(e, [Tag.Parse("order:1")])).ToList());
        var last = await store.AppendAsync(StreamId.Parse("Order:2"), OneEvent().Select(e => new TaggedEvent(e, [Tag.Parse("order:1")])).ToList());

        var query = Query.FromItems(QueryItem.ByTags(Tag.Parse("order:1")));
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
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await store.AppendAsync(StreamId.Parse("Order:1"), OneEvent().Select(e => new TaggedEvent(e, [Tag.Parse("tag:a"), Tag.Parse("tag:only-a")])).ToList());
        var both = await store.AppendAsync(StreamId.Parse("Order:2"), OneEvent().Select(e => new TaggedEvent(e, [Tag.Parse("tag:a"), Tag.Parse("tag:b")])).ToList());
        await store.AppendAsync(StreamId.Parse("Order:3"), OneEvent().Select(e => new TaggedEvent(e, [Tag.Parse("tag:b")])).ToList());

        var max = await store.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags(Tag.Parse("tag:a"), Tag.Parse("tag:b"))));
        Assert.Equal(both.SequencePositions[0], max);
    }

    // ── GetStreamCountAsync ───────────────────────────────────────────────────

    [Fact]
    public async Task GetStreamCountAsync_EmptyStore_ReturnsZero()
    {
        var store = new InMemoryEventStore(enableRegistry: true, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        Assert.Equal(0L, await store.GetStreamCountAsync());
    }

    [Fact]
    public async Task GetStreamCountAsync_AfterMultipleStreams_ReturnsCorrectCount()
    {
        var store = new InMemoryEventStore(enableRegistry: true, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await store.AppendAsync(StreamId.Parse("Order:1"), OneEvent());
        await store.AppendAsync(StreamId.Parse("Order:2"), OneEvent());
        await store.AppendAsync(StreamId.Parse("Customer:1"), OneEvent());
        Assert.Equal(3L, await store.GetStreamCountAsync());
    }

    [Fact]
    public async Task GetStreamCountAsync_AppendingToSameStream_CountsOnce()
    {
        var store = new InMemoryEventStore(enableRegistry: true, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await store.AppendAsync(StreamId.Parse("Order:1"), OneEvent());
        await store.AppendAsync(StreamId.Parse("Order:1"), OneEvent());
        Assert.Equal(1L, await store.GetStreamCountAsync());
    }

    [Fact]
    public async Task GetStreamCountAsync_WithPrefix_ReturnsFilteredCount()
    {
        var store = new InMemoryEventStore(enableRegistry: true, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await store.AppendAsync(StreamId.Parse("Order:1"), OneEvent());
        await store.AppendAsync(StreamId.Parse("Order:2"), OneEvent());
        await store.AppendAsync(StreamId.Parse("Customer:1"), OneEvent());

        Assert.Equal(2L, await store.GetStreamCountAsync("Order:"));
        Assert.Equal(1L, await store.GetStreamCountAsync("Customer:"));
        Assert.Equal(0L, await store.GetStreamCountAsync("Product:"));
    }

    [Fact]
    public async Task GetStreamCountAsync_NullPrefix_ReturnsAllStreams()
    {
        var store = new InMemoryEventStore(enableRegistry: true, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        await store.AppendAsync(StreamId.Parse("Order:1"), OneEvent());
        await store.AppendAsync(StreamId.Parse("Customer:1"), OneEvent());

        Assert.Equal(2L, await store.GetStreamCountAsync(null));
    }

    [Fact]
    public async Task PrefixMatch_IsCaseInsensitive_AndTreatsLikeMetacharactersAsLiteral()
    {
        var store = new InMemoryEventStore(enableRegistry: true, options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Shared, ValidateNames = false });
        await store.AppendAsync(StreamId.Parse("acme_west:Order:1"), OneEvent());
        await store.AppendAsync(StreamId.Parse("acmeXwest:Order:1"), OneEvent());
        await store.AppendAsync(StreamId.Parse("acme%west:Order:1"), OneEvent());
        await store.AppendAsync(StreamId.Parse("acme[west:Order:1"), OneEvent());
        await store.AppendAsync(StreamId.Parse("ACME_WEST:Order:2"), OneEvent());

        var underscore = await store.EnumerateStreamIdsAsync("acme_west:");
        Assert.Equal(2, underscore.Count);
        Assert.Contains("acme_west:Order:1", underscore.Select(static s => s.ToString()).ToList());
        Assert.Contains("ACME_WEST:Order:2", underscore.Select(static s => s.ToString()).ToList());
        Assert.DoesNotContain("acmeXwest:Order:1", underscore.Select(static s => s.ToString()).ToList());

        Assert.Equal(["acme%west:Order:1"], (await store.EnumerateStreamIdsAsync("acme%west:")).Select(static s => s.ToString()));
        Assert.Equal(["acme[west:Order:1"], (await store.EnumerateStreamIdsAsync("acme[west:")).Select(static s => s.ToString()));
        Assert.Equal(2L, await store.GetStreamCountAsync("acme_west:"));
        Assert.Equal(1L, await store.GetStreamCountAsync("acme%west:"));
        Assert.Equal(1L, await store.GetStreamCountAsync("acme[west:"));
        Assert.Equal(2L, await store.GetStreamCountAsync("acme_"));
    }
[EventTypeName("in-memory-event-store-admin-test.test-event")]

    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}
