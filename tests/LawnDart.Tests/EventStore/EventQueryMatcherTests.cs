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
        var raw = new StubRawEvent("test.course-created");
        var se = new SequencedEvent(raw, 1, "s", 1, new EventMetadata());
        var query = Query.FromItems(QueryItem.ByType("test.course-created"));

        Assert.True(EventQueryMatcher.Matches(se, query));
        Assert.False(EventQueryMatcher.Matches(
            se,
            Query.FromItems(QueryItem.ByType("OpaqueEvent"))));
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
    }

    private sealed class StubRawEvent : IRawEvent
    {
        public StubRawEvent(string typeName) => TypeName = typeName;

        public string TypeName { get; }
        public byte[] RawPayload { get; } = [];
        public Guid Id { get; } = Guid.NewGuid();
        public DateTime Timestamp { get; } = DateTime.UtcNow;
    }
}
