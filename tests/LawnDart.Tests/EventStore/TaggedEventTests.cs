using LawnDart.EventStore;
using LawnDart.Metadata;
using NSubstitute;

namespace LawnDart.Tests.EventStore;

public class TaggedEventTests
{
    private sealed record Ping(Guid Id, DateTime Timestamp) : IEvent;

    private static Ping NewPing() => new(Guid.NewGuid(), DateTime.UtcNow);

    [Fact]
    public void Null_event_is_rejected()
    {
        Assert.Throws<ArgumentNullException>(() => new TaggedEvent(null!, "a:1"));
    }

    [Fact]
    public void Null_tags_mean_no_tags()
    {
        var tagged = new TaggedEvent(NewPing(), (IReadOnlyList<string>)null!);
        Assert.Empty(tagged.Tags);
    }

    [Fact]
    public void No_tags_given_means_no_tags()
    {
        Assert.Empty(new TaggedEvent(NewPing()).Tags);
    }

    [Fact]
    public void Repeated_tags_are_kept_once_first_copy_in_order()
    {
        var tagged = new TaggedEvent(NewPing(), "c:1", "a:1", "c:1", "b:1", "a:1");
        Assert.Equal(["c:1", "a:1", "b:1"], tagged.Tags);
    }

    [Fact]
    public void Tags_compare_exactly()
    {
        var tagged = new TaggedEvent(NewPing(), "Course:1", "course:1", "Course:1");
        Assert.Equal(["Course:1", "course:1"], tagged.Tags);
    }

    [Fact]
    public void Tags_are_copied_when_created()
    {
        var source = new List<string> { "a:1", "b:1" };
        var tagged = new TaggedEvent(NewPing(), source);
        source.Add("c:1");
        source[0] = "changed";
        Assert.Equal(["a:1", "b:1"], tagged.Tags);
    }

    [Fact]
    public async Task Stream_extension_wraps_each_event_with_no_tags()
    {
        var store = Substitute.For<IEventStore>();
        IEnumerable<TaggedEvent>? captured = null;
        store.AppendAsync("s-1", Arg.Do<IEnumerable<TaggedEvent>>(e => captured = e.ToList()), 3, Arg.Any<EventMetadata?>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new AppendResult([1, 2])));
        var (first, second) = (NewPing(), NewPing());
        var metadata = new EventMetadata();

        await store.AppendAsync("s-1", [first, second], expectedVersion: 3, metadata: metadata);

        Assert.NotNull(captured);
        var list = captured!.ToList();
        Assert.Equal([first, second], list.Select(t => t.Event));
        Assert.All(list, t => Assert.Empty(t.Tags));
        await store.Received(1).AppendAsync("s-1", Arg.Any<IEnumerable<TaggedEvent>>(), 3, metadata, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Condition_extension_wraps_each_event_with_no_tags()
    {
        var store = Substitute.For<IEventStore>();
        IEnumerable<TaggedEvent>? captured = null;
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags("a:1")));
        store.AppendAsync(Arg.Do<IEnumerable<TaggedEvent>>(e => captured = e.ToList()), condition, Arg.Any<EventMetadata?>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new AppendResult([1])));
        var evt = NewPing();

        await store.AppendAsync([evt], condition);

        var only = Assert.Single(captured!);
        Assert.Same(evt, only.Event);
        Assert.Empty(only.Tags);
    }

    [Fact]
    public async Task Extensions_reject_null_arguments()
    {
        var store = Substitute.For<IEventStore>();
        await Assert.ThrowsAsync<ArgumentNullException>(() => EventStoreAppendExtensions.AppendAsync(null!, "s", [NewPing()]));
        await Assert.ThrowsAsync<ArgumentNullException>(() => store.AppendAsync("s", (IEnumerable<IEvent>)null!));
    }
}
