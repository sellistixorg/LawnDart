using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Serialization;

namespace LawnDart.Tests.EventStore;

public class EventSessionTests
{
    [Fact]
    public void ToAppendEvent_stamps_schema_version_one_and_stj_payload()
    {
        var catalog = MapCatalog.For<AuthorRegistered>("author-registered");
        var session = new EventSession(new StjEventSerializer(), catalog);
        var evt = new AuthorRegistered(Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc), "Ada");

        var append = session.ToAppendEvent(evt, new EventMetadata { UserId = "u-1" }, ["author:1"]);

        Assert.Equal("author-registered", append.EventType);
        Assert.Equal(1, append.SchemaVersion);
        Assert.Equal("application/json", append.ContentType);
        Assert.Equal(["author:1"], append.Tags);
        var json = Encoding.UTF8.GetString(append.Payload.Span);
        Assert.StartsWith("{", json);
        Assert.Contains("\"Name\":\"Ada\"", json);
        var meta = JsonSerializer.Deserialize<EventMetadata>(append.Metadata.Span, StjEventSerializer.Options);
        Assert.Equal(1, meta!.SchemaVersion);
        Assert.Equal("author-registered", meta.SchemaName);
        Assert.Equal("u-1", meta.UserId);
    }

    [Fact]
    public void ToAppendEvent_without_metadata_copies_event_id_and_timestamp()
    {
        var catalog = MapCatalog.For<AuthorRegistered>("author-registered");
        var session = new EventSession(new StjEventSerializer(), catalog);
        var id = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        var timestamp = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc);

        var append = session.ToAppendEvent(new AuthorRegistered(id, timestamp, "Ada"));
        var meta = JsonSerializer.Deserialize<EventMetadata>(append.Metadata.Span, StjEventSerializer.Options);

        Assert.Equal(id.ToString(), meta!.EventId);
        Assert.Equal(timestamp, meta.Timestamp);
        Assert.Equal("author-registered", meta.SchemaName);
        Assert.Equal(1, meta.SchemaVersion);
        Assert.Null(meta.CommitTimestamp);
    }

    [Fact]
    public void ToAppendEvent_does_not_mutate_caller_metadata()
    {
        var catalog = MapCatalog.For<AuthorRegistered>("author-registered");
        var session = new EventSession(new StjEventSerializer(), catalog);
        var metadata = new EventMetadata { SchemaVersion = 7, UserId = "u-1" };

        var append = session.ToAppendEvent(new AuthorRegistered(Guid.NewGuid(), DateTime.UtcNow, "Ada"), metadata);
        metadata.UserId = "mutated";
        metadata.SchemaVersion = 99;

        var stored = JsonSerializer.Deserialize<EventMetadata>(append.Metadata.Span, StjEventSerializer.Options);
        Assert.Equal("u-1", stored!.UserId);
        Assert.Equal(1, stored.SchemaVersion);
        Assert.Equal(99, metadata.SchemaVersion);
    }

    [Fact]
    public void Hydrate_copies_frame_commit_timestamp_and_schema_version()
    {
        var catalog = MapCatalog.For<AuthorRegistered>("author-registered");
        var session = new EventSession(new StjEventSerializer(), catalog);
        var evt = new AuthorRegistered(Guid.NewGuid(), DateTime.UtcNow, "Ada");
        var payload = new StjEventSerializer().Serialize(evt, evt.GetType());
        var blob = JsonSerializer.SerializeToUtf8Bytes(new EventMetadata
        {
            UserId = "u-1",
            CommitTimestamp = null,
            SchemaVersion = 99
        }, StjEventSerializer.Options);
        var commit = new DateTime(2026, 9, 16, 12, 0, 0, DateTimeKind.Utc);

        var recorded = new RecordedEvent(
            "author-registered",
            payload,
            "authors-1",
            streamVersion: 3,
            sequencePosition: 42,
            commitTimestamp: commit,
            blob,
            schemaVersion: 1);

        var sequenced = session.Hydrate(recorded);

        Assert.Equal(42, sequenced.SequencePosition);
        Assert.Equal(3, sequenced.Version);
        Assert.Equal("authors-1", sequenced.StreamId);
        var hydrated = Assert.IsType<AuthorRegistered>(sequenced.Event);
        Assert.Equal("Ada", hydrated.Name);
        Assert.Equal(commit, sequenced.Metadata.CommitTimestamp);
        Assert.Equal(1, sequenced.Metadata.SchemaVersion);
        Assert.Equal("u-1", sequenced.Metadata.UserId);
        Assert.NotNull(sequenced.Metadata.CommitTimestamp);
    }

    [Fact]
    public void Hydrate_empty_metadata_blob_still_sets_commit_timestamp()
    {
        var catalog = MapCatalog.For<AuthorRegistered>("author-registered");
        var session = new EventSession(new StjEventSerializer(), catalog);
        var evt = new AuthorRegistered(Guid.NewGuid(), DateTime.UtcNow, "Ada");
        var commit = new DateTime(2026, 9, 16, 15, 0, 0, DateTimeKind.Utc);

        var recorded = new RecordedEvent(
            "author-registered",
            new StjEventSerializer().Serialize(evt, evt.GetType()),
            "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: commit);

        var sequenced = session.Hydrate(recorded);

        Assert.Equal(commit, sequenced.Metadata.CommitTimestamp);
        Assert.Equal(1, sequenced.Metadata.SchemaVersion);
        Assert.NotNull(sequenced.Metadata.CommitTimestamp);
    }

    [Fact]
    public void Hydrate_content_type_mismatch_throws_without_migration_tool()
    {
        var catalog = MapCatalog.For<AuthorRegistered>("author-registered");
        var session = new EventSession(new StjEventSerializer(), catalog);
        var evt = new AuthorRegistered(Guid.NewGuid(), DateTime.UtcNow, "Ada");
        var recorded = new RecordedEvent(
            "author-registered",
            new StjEventSerializer().Serialize(evt, evt.GetType()),
            "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow,
            contentType: "application/avro");

        var ex = Assert.Throws<EventContentTypeMismatchException>(() => session.Hydrate(recorded));
        Assert.Equal("application/avro", ex.StoredContentType);
        Assert.Equal("application/json", ex.SessionContentType);
        Assert.Contains("application/avro", ex.Message);
        Assert.DoesNotContain("migration tool", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.IsAssignableFrom<EventHydrationException>(ex);
    }

    [Fact]
    public void Hydrate_unknown_family_fails_closed_and_is_not_a_raw_wrapper()
    {
        var catalog = MapCatalog.For<AuthorRegistered>("author-registered");
        var session = new EventSession(new StjEventSerializer(), catalog);
        var recorded = new RecordedEvent(
            "foreign-family",
            Encoding.UTF8.GetBytes("""{"kind":"opaque"}"""),
            "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow);

        var ex = Assert.Throws<UnknownEventFamilyException>(() => session.Hydrate(recorded));
        Assert.Equal("foreign-family", ex.FamilyToken);
        Assert.Contains("foreign-family", ex.Message);
        Assert.DoesNotContain(nameof(RawRecordedEvent), ex.Message, StringComparison.Ordinal);
        Assert.Contains("skip override", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Hydrate_SchemaVersionNewerThanProcess_ThrowsTooNew()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(AuthorRegistered)]);
        var session = new EventSession(new StjEventSerializer(), catalog);
        var recorded = new RecordedEvent(
            "author-registered",
            Encoding.UTF8.GetBytes("""{"Id":"00000000-0000-0000-0000-000000000000"}"""),
            "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow,
            schemaVersion: 99);

        var ex = Assert.Throws<EventSchemaTooNewException>(() => session.Hydrate(recorded));
        Assert.Equal("author-registered", ex.FamilyToken);
        Assert.Equal(99, ex.SchemaVersion);
        Assert.Equal(1, ex.ProcessCurrentVersion);
        Assert.Contains("skip override", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Hydrate_KnownFamilyMissingHistoricalType_ThrowsNotInCatalog()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(AuthorEvolved)]);
        var session = new EventSession(new StjEventSerializer(), catalog);
        var recorded = new RecordedEvent(
            "author-evolved",
            Encoding.UTF8.GetBytes("""{"Id":"00000000-0000-0000-0000-000000000000"}"""),
            "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow,
            schemaVersion: 1);

        var ex = Assert.Throws<EventSchemaNotInCatalogException>(() => session.Hydrate(recorded));
        Assert.Equal("author-evolved", ex.FamilyToken);
        Assert.Equal(1, ex.SchemaVersion);
        Assert.Contains("skip override", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Hydrate_MalformedPayload_ThrowsPayloadException()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(AuthorRegistered)]);
        var session = new EventSession(new StjEventSerializer(), catalog);
        var recorded = new RecordedEvent(
            "author-registered",
            Encoding.UTF8.GetBytes("not-json"),
            "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow,
            schemaVersion: 1);

        var ex = Assert.Throws<EventPayloadException>(() => session.Hydrate(recorded));
        Assert.Equal("author-registered", ex.FamilyToken);
        Assert.Equal(1, ex.SchemaVersion);
        Assert.NotNull(ex.InnerException);
    }

    [Fact]
    public void Fake_backend_never_resolves_CLR_types()
    {
        var catalog = MapCatalog.For<AuthorRegistered>("author-registered");
        var session = new EventSession(new StjEventSerializer(), catalog);
        var log = new FrameOnlyLog();
        var evt = new AuthorRegistered(Guid.NewGuid(), DateTime.UtcNow, "Ada");

        var append = session.ToAppendEvent(evt, new EventMetadata { UserId = "u-1" });
        Assert.Equal(1, catalog.ResolveCalls);

        var recorded = log.Commit("authors-1", append);
        Assert.Equal(0, log.TypeResolves);
        Assert.Equal("author-registered", recorded.EventType);

        var sequenced = session.Hydrate(recorded);

        Assert.Equal(3, catalog.ResolveCalls);
        Assert.Equal(0, log.TypeResolves);
        Assert.IsType<AuthorRegistered>(sequenced.Event);
        Assert.Equal(recorded.CommitTimestamp, sequenced.Metadata.CommitTimestamp);
    }

    [Fact]
    public void ToAppendEvent_HistoricalType_Throws()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(AuthorEvolvedV1), typeof(AuthorEvolved)]);
        var session = new EventSession(new StjEventSerializer(), catalog);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            session.ToAppendEvent(new AuthorEvolvedV1(Guid.NewGuid(), DateTime.UtcNow, "Ada")));

        Assert.Contains("historical", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("author-evolved", ex.Message);
        Assert.Contains(nameof(AuthorEvolved), ex.Message);
    }

    [Fact]
    public void ToAppendEvent_CurrentType_StillAppends()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(AuthorEvolvedV1), typeof(AuthorEvolved)]);
        var session = new EventSession(new StjEventSerializer(), catalog);

        var append = session.ToAppendEvent(new AuthorEvolved(Guid.NewGuid(), DateTime.UtcNow, "Ada", "bio"));

        Assert.Equal("author-evolved", append.EventType);
        Assert.Equal(2, append.SchemaVersion);
    }

    [Fact]
    public void ToAppendEvent_stamps_frame_and_metadata_from_declared_version()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(AuthorEvolvedV1), typeof(AuthorEvolved)]);
        var session = new EventSession(new StjEventSerializer(), catalog);
        var caller = new EventMetadata { SchemaVersion = 99, UserId = "u-1" };

        var append = session.ToAppendEvent(
            new AuthorEvolved(Guid.NewGuid(), DateTime.UtcNow, "Ada", "bio"),
            caller);

        Assert.Equal(2, append.SchemaVersion);
        var stored = JsonSerializer.Deserialize<EventMetadata>(append.Metadata.Span, StjEventSerializer.Options);
        Assert.Equal(2, stored!.SchemaVersion);
        Assert.Equal(99, caller.SchemaVersion);
        Assert.Equal("u-1", stored.UserId);
    }

    [Fact]
    public void Hydrate_v1_without_pipeline_throws_missing_upcaster()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(AuthorEvolvedV1), typeof(AuthorEvolved)]);
        var session = new EventSession(new StjEventSerializer(), catalog);
        var v1 = new AuthorEvolvedV1(Guid.NewGuid(), DateTime.UtcNow, "Ada");
        var recorded = new RecordedEvent(
            "author-evolved",
            new StjEventSerializer().Serialize(v1, v1.GetType()),
            "authors-1",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow,
            schemaVersion: 1);

        var ex = Assert.Throws<MissingEventUpcasterException>(() => session.Hydrate(recorded));
        Assert.Equal("author-evolved", ex.FamilyToken);
        Assert.Equal(1, ex.FromVersion);
        Assert.Equal(2, ex.ToVersion);
        Assert.Contains("skip override", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Hydrate_upcaster_throw_is_payload_exception()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(AuthorEvolvedV1), typeof(AuthorEvolved)]);
        var pipeline = EventUpcastPipeline.Materialize(catalog, [typeof(ThrowingAuthorUpcaster)]);
        var session = new EventSession(new StjEventSerializer(), catalog, pipeline);
        var v1 = new AuthorEvolvedV1(Guid.NewGuid(), DateTime.UtcNow, "Ada");
        var recorded = new RecordedEvent(
            "author-evolved",
            new StjEventSerializer().Serialize(v1, v1.GetType()),
            "authors-1",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow,
            schemaVersion: 1);

        var ex = Assert.Throws<EventPayloadException>(() => session.Hydrate(recorded));
        Assert.Contains("Upcast failed", ex.Message);
        Assert.IsType<InvalidOperationException>(ex.InnerException);
    }

    [Fact]
    public void Hydrate_uses_frame_schema_version_not_metadata_blob()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(AuthorEvolvedV1), typeof(AuthorEvolved)]);
        var pipeline = EventUpcastPipeline.Materialize(catalog, [typeof(AuthorEvolvedV1ToCurrent)]);
        var session = new EventSession(new StjEventSerializer(), catalog, pipeline);
        var v1 = new AuthorEvolvedV1(Guid.NewGuid(), DateTime.UtcNow, "Ada");
        var payload = new StjEventSerializer().Serialize(v1, v1.GetType());
        var blob = JsonSerializer.SerializeToUtf8Bytes(new EventMetadata
        {
            SchemaVersion = 2,
            UserId = "u-1"
        }, StjEventSerializer.Options);

        var recorded = new RecordedEvent(
            "author-evolved",
            payload,
            "authors-1",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow,
            blob,
            schemaVersion: 1);

        var sequenced = session.Hydrate(recorded);

        var current = Assert.IsType<AuthorEvolved>(sequenced.Event);
        Assert.Equal("Ada", current.Name);
        Assert.Equal("", current.Bio);
        Assert.Equal(1, sequenced.Metadata.SchemaVersion);
        Assert.Equal("u-1", sequenced.Metadata.UserId);
    }

    [Fact]
    public void Hydrate_uses_frame_commit_timestamp_not_metadata_blob()
    {
        var catalog = MapCatalog.For<AuthorRegistered>("author-registered");
        var session = new EventSession(new StjEventSerializer(), catalog);
        var evt = new AuthorRegistered(Guid.NewGuid(), DateTime.UtcNow, "Ada");
        var blobTime = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var frameTime = new DateTime(2026, 9, 16, 18, 0, 0, DateTimeKind.Utc);
        var blob = JsonSerializer.SerializeToUtf8Bytes(new EventMetadata
        {
            CommitTimestamp = blobTime,
            SchemaVersion = 7
        }, StjEventSerializer.Options);

        var recorded = new RecordedEvent(
            "author-registered",
            new StjEventSerializer().Serialize(evt, evt.GetType()),
            "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: frameTime,
            blob,
            schemaVersion: 1);

        var sequenced = session.Hydrate(recorded);

        Assert.Equal(frameTime, sequenced.Metadata.CommitTimestamp);
        Assert.Equal(1, sequenced.Metadata.SchemaVersion);
        Assert.NotEqual(blobTime, sequenced.Metadata.CommitTimestamp);
    }

    [EventTypeName("author-registered")]
    private sealed record AuthorRegistered(Guid Id, DateTime Timestamp, string Name) : IEvent;

    [EventTypeName("author-evolved", version: 1)]
    public sealed record AuthorEvolvedV1(Guid Id, DateTime Timestamp, string Name) : IEvent;

    [EventTypeName("author-evolved", version: 2, current: true)]
    public sealed record AuthorEvolved(Guid Id, DateTime Timestamp, string Name, string Bio) : IEvent;

    public sealed class AuthorEvolvedV1ToCurrent : IEventUpcaster<AuthorEvolved, AuthorEvolvedV1>
    {
        public AuthorEvolved Upcast(AuthorEvolvedV1 source)
            => new(source.Id, source.Timestamp, source.Name, "");
    }

    public sealed class ThrowingAuthorUpcaster : IEventUpcaster<AuthorEvolved, AuthorEvolvedV1>
    {
        public AuthorEvolved Upcast(AuthorEvolvedV1 source)
            => throw new InvalidOperationException("upcaster boom");
    }

    private sealed class StjEventSerializer : IEventSerializer
    {
        internal static readonly JsonSerializerOptions Options = new()
        {
            PropertyNameCaseInsensitive = true,
            WriteIndented = false,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        public string ContentType => "application/json";

        public ReadOnlyMemory<byte> Serialize(object obj, Type type)
            => JsonSerializer.SerializeToUtf8Bytes(obj, type, Options);

        public object Deserialize(ReadOnlyMemory<byte> data, Type type)
            => JsonSerializer.Deserialize(data.Span, type, Options)
               ?? throw new InvalidOperationException($"Failed to deserialize {type.Name}");
    }

    private sealed class MapCatalog : IEventTypeCatalog
    {
        private readonly Dictionary<Type, string> _typeToToken = new();
        private readonly Dictionary<string, Type> _tokenToType = new(StringComparer.Ordinal);

        public int ResolveCalls { get; private set; }

        public static MapCatalog For<TEvent>(string token) where TEvent : IEvent
        {
            var catalog = new MapCatalog();
            catalog._typeToToken[typeof(TEvent)] = token;
            catalog._tokenToType[token] = typeof(TEvent);
            return catalog;
        }

        public string GetName(Type type) => _typeToToken[type];

        public bool TryResolveType(string storedName, [NotNullWhen(true)] out Type? type)
        {
            ResolveCalls++;
            return _tokenToType.TryGetValue(storedName, out type);
        }
    }

    /// <summary>
    /// Schema-dumb log double. Accepts only <see cref="AppendEvent"/>; has no catalog.
    /// A design that resolved <see cref="Type"/> inside this backend would have to
    /// add a resolve call here and fail <c>TypeResolves == 0</c>.
    /// </summary>
    private sealed class FrameOnlyLog
    {
        public int TypeResolves { get; private set; }

        public RecordedEvent Commit(string streamId, AppendEvent envelope)
        {
            return new RecordedEvent(
                envelope.EventType,
                envelope.Payload,
                streamId,
                streamVersion: 1,
                sequencePosition: 1,
                commitTimestamp: new DateTime(2026, 9, 16, 12, 0, 0, DateTimeKind.Utc),
                envelope.Metadata,
                envelope.SchemaVersion,
                envelope.ContentType,
                envelope.Tags);
        }
    }
}
