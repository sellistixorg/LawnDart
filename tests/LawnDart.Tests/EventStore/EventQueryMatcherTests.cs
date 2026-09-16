using LawnDart;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Xunit;

namespace LawnDart.Tests.EventStore;

public class EventQueryMatcherTests
{
    [Fact]
    public void ByType_matches_IRawEvent_TypeName_not_CLR_name()
    {
        var recorded = new RecordedEvent(
            "test.course-created",
            ReadOnlyMemory<byte>.Empty,
            "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow);
        var raw = RawRecordedEvent.From(recorded);
        var se = new SequencedEvent(raw, 1, "s", 1, new EventMetadata());
        var query = Query.FromItems(QueryItem.ByType("test.course-created"));

        Assert.True(EventQueryMatcher.Matches(se, query));
        Assert.False(EventQueryMatcher.Matches(
            se,
            Query.FromItems(QueryItem.ByType(nameof(RawRecordedEvent)))));
    }

    [EventTypeName("named.tick")]
    private sealed record NamedTick(Guid Id, DateTime Timestamp) : IEvent;

    [Fact]
    public void ByType_still_matches_attributed_CLR_events()
    {
        var se = new SequencedEvent(
            new NamedTick(Guid.NewGuid(), DateTime.UtcNow),
            1,
            "s",
            1,
            new EventMetadata());

        Assert.True(EventQueryMatcher.Matches(
            se,
            Query.FromItems(QueryItem.ByType("named.tick"))));
        Assert.False(EventQueryMatcher.Matches(
            se,
            Query.FromItems(QueryItem.ByType(typeof(NamedTick).FullName!))));
    }

    [Fact]
    public void ByType_on_recorded_event_uses_stored_token()
    {
        var recorded = new RecordedEvent(
            "named.tick",
            ReadOnlyMemory<byte>.Empty,
            "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow);

        Assert.True(EventQueryMatcher.Matches(
            recorded,
            Query.FromItems(QueryItem.ByType("named.tick"))));
        Assert.False(EventQueryMatcher.Matches(
            recorded,
            Query.FromItems(QueryItem.ByType("NamedTick"))));
    }
}
