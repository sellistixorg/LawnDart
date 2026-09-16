using System.Diagnostics.CodeAnalysis;
using LawnDart.EventSourcing.Serialization;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.EventSourcing.Tests.EventStore;

public class EventSessionJsonTests
{
    [Fact]
    public void JsonEventSerializer_round_trips_through_EventSession()
    {
        var catalog = new IsolatedCatalog();
        var session = new EventSession(new JsonEventSerializer(), catalog);
        var evt = new BookRegistered(Guid.NewGuid(), DateTime.UtcNow, "Dune");
        var commit = new DateTime(2026, 9, 16, 18, 0, 0, DateTimeKind.Utc);

        var append = session.ToAppendEvent(evt, new EventMetadata { UserId = "u-1" });
        var recorded = new RecordedEvent(
            append.EventType,
            append.Payload,
            "books-1",
            streamVersion: 1,
            sequencePosition: 9,
            commitTimestamp: commit,
            append.Metadata,
            append.SchemaVersion,
            append.ContentType,
            append.Tags);

        var sequenced = session.Hydrate(recorded);

        Assert.Equal("book-registered", append.EventType);
        Assert.Equal("application/json", append.ContentType);
        var hydrated = Assert.IsType<BookRegistered>(sequenced.Event);
        Assert.Equal("Dune", hydrated.Title);
        Assert.Equal(commit, sequenced.Metadata.CommitTimestamp);
        Assert.Equal(1, sequenced.Metadata.SchemaVersion);
    }

    [EventTypeName("book-registered")]
    private sealed record BookRegistered(Guid Id, DateTime Timestamp, string Title) : IEvent;

    private sealed class IsolatedCatalog : IEventTypeCatalog
    {
        public string GetName(Type type) => EventTypeNameResolver.TryGetDeclaredName(type)
            ?? throw new InvalidOperationException(type.FullName);

        public bool TryResolveType(string storedName, [NotNullWhen(true)] out Type? type)
        {
            if (storedName == "book-registered")
            {
                type = typeof(BookRegistered);
                return true;
            }

            type = null;
            return false;
        }
    }
}
