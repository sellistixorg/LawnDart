using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Xunit;

namespace LawnDart.EventSourcing.Tests.EventStore;

/// <summary>
/// InMemory mirror tests for <see cref="InMemoryEventStore.ReadByQueryStreamAsync"/>.
/// Covers the four key behaviours: tag match, sequence bounds, break-early, and empty result.
/// </summary>
public class InMemoryReadByQueryStreamTests
{
    private static async Task<List<SequencedEvent>> DrainAsync(IAsyncEnumerable<SequencedEvent> stream)
    {
        var list = new List<SequencedEvent>();
        await foreach (var evt in stream)
            list.Add(evt);
        return list;
    }

    [Fact]
    public async Task ReadByQueryStreamAsync_ByTags_YieldsMatchingEventsInOrder()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("stream:alpha:1", new[] { (IEvent)new TestStreamEvent("A") }, tags: new[] { "tag:alpha" });
        await store.AppendAsync("stream:beta:1",  new[] { (IEvent)new TestStreamEvent("B") }, tags: new[] { "tag:beta" });
        await store.AppendAsync("stream:alpha:2", new[] { (IEvent)new TestStreamEvent("C") }, tags: new[] { "tag:alpha" });

        var events = await DrainAsync(store.ReadByQueryStreamAsync(
            Query.FromItems(QueryItem.ByTags("tag:alpha"))));

        Assert.Equal(2, events.Count);
        Assert.All(events, e => Assert.Contains("tag:alpha", e.Tags));
        Assert.True(events[0].SequencePosition < events[1].SequencePosition);
    }

    [Fact]
    public async Task ReadByQueryStreamAsync_EmptyResult_YieldsNothing()
    {
        var store = new InMemoryEventStore();

        var events = await DrainAsync(store.ReadByQueryStreamAsync(
            Query.FromItems(QueryItem.ByTags("tag:missing"))));

        Assert.Empty(events);
    }

    [Fact]
    public async Task ReadByQueryStreamAsync_CanBreakEarly()
    {
        var store = new InMemoryEventStore();
        for (var i = 0; i < 5; i++)
            await store.AppendAsync(
                $"stream:early:{i}",
                new[] { (IEvent)new TestStreamEvent($"item-{i}") },
                tags: new[] { "tag:early" });

        SequencedEvent? first = null;
        await foreach (var evt in store.ReadByQueryStreamAsync(
            Query.FromItems(QueryItem.ByTags("tag:early"))))
        {
            first = evt;
            break;
        }

        Assert.NotNull(first);
    }

    [Fact]
    public async Task ReadByQueryStreamAsync_ResultsMatchReadByQueryAsync()
    {
        var store = new InMemoryEventStore();
        for (var i = 0; i < 4; i++)
            await store.AppendAsync(
                $"stream:parity:{i}",
                new[] { (IEvent)new TestStreamEvent($"parity-{i}") },
                tags: new[] { "tag:parity" });

        var buffered = (await store.ReadByQueryAsync(
            Query.FromItems(QueryItem.ByTags("tag:parity")))).Events;

        var streamed = await DrainAsync(store.ReadByQueryStreamAsync(
            Query.FromItems(QueryItem.ByTags("tag:parity"))));

        Assert.Equal(buffered.Count, streamed.Count);
        for (var i = 0; i < buffered.Count; i++)
            Assert.Equal(buffered[i].SequencePosition, streamed[i].SequencePosition);
    }

    private record TestStreamEvent(string Name) : IEvent
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    }
}
