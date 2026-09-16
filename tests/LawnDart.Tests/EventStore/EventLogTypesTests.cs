using System.Reflection;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Tests.EventStore;

public class EventLogTypesTests
{
    [Fact]
    public void AppendEvent_has_no_store_assigned_fields()
    {
        var type = typeof(AppendEvent);

        Assert.Null(type.GetProperty("SequencePosition"));
        Assert.Null(type.GetProperty("GlobalSequence"));
        Assert.Null(type.GetProperty("StreamId"));
        Assert.Null(type.GetProperty("StreamVersion"));
        Assert.Null(type.GetProperty("Version"));
        Assert.Null(type.GetProperty("CommitTimestamp"));
        Assert.Null(type.GetProperty(nameof(EventMetadata)));
        Assert.Null(type.GetProperty("Metadata", BindingFlags.Public | BindingFlags.Instance)
            ?.PropertyType.GetProperty(nameof(EventMetadata.SchemaVersion)));
        Assert.Equal(typeof(ReadOnlyMemory<byte>), type.GetProperty(nameof(AppendEvent.Metadata))!.PropertyType);
    }

    [Fact]
    public void RecordedEvent_carries_store_assigned_fields_and_first_class_schema_version()
    {
        var recorded = new RecordedEvent(
            eventType: "author-registered",
            payload: new byte[] { 1 },
            streamId: "authors-1",
            streamVersion: 3,
            sequencePosition: 42,
            commitTimestamp: new DateTime(2026, 9, 16, 0, 0, 0, DateTimeKind.Utc),
            schemaVersion: 2);

        Assert.Equal(42, recorded.SequencePosition);
        Assert.Equal(3, recorded.StreamVersion);
        Assert.Equal("authors-1", recorded.StreamId);
        Assert.Equal(2, recorded.SchemaVersion);
        Assert.Equal(new DateTime(2026, 9, 16, 0, 0, 0, DateTimeKind.Utc), recorded.CommitTimestamp);
        Assert.Equal(typeof(ReadOnlyMemory<byte>), recorded.Metadata.GetType());
    }

    [Fact]
    public void AppendEvent_defaults_schema_version_one_and_json_content_type()
    {
        var append = new AppendEvent("author-registered", payload: new byte[] { 1 });

        Assert.Equal(1, append.SchemaVersion);
        Assert.Equal(AppendEvent.DefaultContentType, append.ContentType);
        Assert.Equal("application/json", append.ContentType);
    }

    [Fact]
    public void AppendEvent_treats_zero_schema_version_as_one()
    {
        var append = new AppendEvent("author-registered", payload: ReadOnlyMemory<byte>.Empty, schemaVersion: 0);
        Assert.Equal(1, append.SchemaVersion);
    }

    [Fact]
    public void AppendEvent_snapshots_payload_and_metadata()
    {
        var payload = new byte[] { 1, 2, 3 };
        var metadata = new byte[] { 9, 8, 7 };

        var append = new AppendEvent(
            "author-registered",
            payload,
            metadata);

        payload[0] = 99;
        metadata[0] = 99;

        Assert.Equal(new byte[] { 1, 2, 3 }, append.Payload.ToArray());
        Assert.Equal(new byte[] { 9, 8, 7 }, append.Metadata.ToArray());
    }

    [Fact]
    public void RecordedEvent_snapshots_payload_and_metadata()
    {
        var payload = new byte[] { 4, 5 };
        var metadata = new byte[] { 6 };

        var recorded = new RecordedEvent(
            "author-registered",
            payload,
            "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow,
            metadata);

        payload[0] = 0;
        metadata[0] = 0;

        Assert.Equal(new byte[] { 4, 5 }, recorded.Payload.ToArray());
        Assert.Equal(new byte[] { 6 }, recorded.Metadata.ToArray());
    }

    [Fact]
    public void EventQueryMatcher_matches_RecordedEvent_family_token_without_CLR_type()
    {
        var recorded = new RecordedEvent(
            "author-registered",
            payload: ReadOnlyMemory<byte>.Empty,
            streamId: "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow,
            tags: ["author:1"]);

        Assert.True(EventQueryMatcher.Matches(
            recorded,
            Query.FromItems(QueryItem.ByType("author-registered"))));
        Assert.False(EventQueryMatcher.Matches(
            recorded,
            Query.FromItems(QueryItem.ByType("book-registered"))));
        Assert.True(EventSubscriptionFilter.ForQuery(
            Query.FromItems(QueryItem.ByType("author-registered"))).Matches(recorded));
    }

    [Fact]
    public void IEventLog_surface_has_no_IEvent_or_EventMetadata()
    {
        var methods = typeof(IEventLog).GetMethods(
            BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
        var names = methods.Select(m => m.Name).ToHashSet(StringComparer.Ordinal);
        Assert.Contains("AppendAsync", names);
        Assert.Contains("ReadStreamAsync", names);
        Assert.Contains("ReadByQueryAsync", names);
        Assert.Contains("GetCurrentSequenceAsync", names);
        Assert.Contains("GetMaxSequencePositionAsync", names);

        var all = methods.SelectMany(Flatten).ToArray();
        Assert.DoesNotContain(typeof(IEvent), all);
        Assert.DoesNotContain(typeof(EventMetadata), all);
        Assert.DoesNotContain(typeof(SequencedEvent), all);
        Assert.Contains(typeof(AppendEvent), all);
        Assert.Contains(typeof(RecordedEvent), all);
        Assert.Contains(typeof(EventLogQueryResult), all);
        Assert.Contains(typeof(AppendResult), all);
    }

    private static IEnumerable<Type> Flatten(MethodInfo method)
    {
        foreach (var type in FlattenType(method.ReturnType))
            yield return type;
        foreach (var arg in method.GetParameters())
        {
            foreach (var type in FlattenType(arg.ParameterType))
                yield return type;
        }
    }

    private static IEnumerable<Type> FlattenType(Type type)
    {
        yield return type;
        if (!type.IsGenericType)
            yield break;
        foreach (var arg in type.GetGenericArguments())
        {
            foreach (var inner in FlattenType(arg))
                yield return inner;
        }
    }
}
