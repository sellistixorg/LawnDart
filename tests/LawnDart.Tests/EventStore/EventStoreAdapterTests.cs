using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Serialization;
using NSubstitute;

namespace LawnDart.Tests.EventStore;

public class EventStoreAdapterTests
{
    [Fact]
    public async Task Subscribe_hydrates_frames_from_a_log_that_has_no_catalog()
    {
        var catalog = MapCatalog.For<AuthorRegistered>("author-registered");
        var session = new EventSession(new StjEventSerializer(), catalog);
        var logHandle = new FrameHandle("sub-1");
        var log = Substitute.For<IEventLog, IEventLogSubscriptions>();
        ((IEventLogSubscriptions)log)
            .Subscribe("sub-1", 1, Arg.Any<EventSubscriptionFilter?>(), Arg.Any<CancellationToken>())
            .Returns(logHandle);

        var evt = new AuthorRegistered(Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc), "Ada");
        var envelope = session.ToAppendEvent(evt);
        logHandle.Write(new RecordedEvent(
            envelope.EventType,
            envelope.Payload,
            "authors-1",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: new DateTime(2026, 9, 16, 12, 0, 0, DateTimeKind.Utc),
            envelope.Metadata,
            envelope.SchemaVersion,
            envelope.ContentType,
            envelope.Tags));
        logHandle.Complete();

        var adapter = new EventStoreAdapter(log, session, (IEventLogSubscriptions)log);
        await using var handle = adapter.Subscribe("sub-1", fromSequence: 1);

        var received = await handle.Events.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsType<AuthorRegistered>(received.Event);
        Assert.Equal("Ada", ((AuthorRegistered)received.Event).Name);
        Assert.Equal(1, handle.LastDeliveredSequence);
        Assert.True(catalog.ResolveCalls > 0);
    }

    [Fact]
    public async Task Subscribe_fail_closed_hydrate_does_not_advance_typed_cursor()
    {
        var catalog = MapCatalog.For<AuthorRegistered>("author-registered");
        var session = new EventSession(new StjEventSerializer(), catalog);
        var logHandle = new FrameHandle("sub-fail");
        var log = Substitute.For<IEventLog, IEventLogSubscriptions>();
        ((IEventLogSubscriptions)log)
            .Subscribe("sub-fail", 1, Arg.Any<EventSubscriptionFilter?>(), Arg.Any<CancellationToken>())
            .Returns(logHandle);

        var evt = new AuthorRegistered(Guid.NewGuid(), DateTime.UtcNow, "Ada");
        var envelope = session.ToAppendEvent(evt);
        logHandle.Write(new RecordedEvent(
            envelope.EventType,
            envelope.Payload,
            "authors-1",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow,
            envelope.Metadata,
            envelope.SchemaVersion,
            envelope.ContentType,
            envelope.Tags));
        logHandle.Write(new RecordedEvent(
            "no-such-family",
            Encoding.UTF8.GetBytes("""{"Id":"00000000-0000-0000-0000-000000000000"}"""),
            "authors-1",
            streamVersion: 2,
            sequencePosition: 2,
            commitTimestamp: DateTime.UtcNow,
            Encoding.UTF8.GetBytes("{}"),
            schemaVersion: 1,
            contentType: "application/json"));
        logHandle.Complete();

        var adapter = new EventStoreAdapter(log, session, (IEventLogSubscriptions)log);
        await using var handle = adapter.Subscribe("sub-fail", fromSequence: 1);

        var first = await handle.Events.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsType<AuthorRegistered>(first.Event);

        var ex = await Assert.ThrowsAsync<UnknownEventFamilyException>(async () =>
        {
            await foreach (var _ in handle.Events.ReadAllAsync())
            {
            }
        }).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Contains("no-such-family", ex.Message);
        Assert.Equal(1, handle.LastDeliveredSequence);
    }

    [Fact]
    public void Subscribe_throws_when_log_has_no_subscription_surface()
    {
        var session = new EventSession(new StjEventSerializer(), MapCatalog.For<AuthorRegistered>("author-registered"));
        var adapter = new EventStoreAdapter(Substitute.For<IEventLog>(), session);

        var ex = Assert.Throws<InvalidOperationException>(() => adapter.Subscribe("sub", 1));
        Assert.Contains("IEventLogSubscriptions", ex.Message);
    }

    [Fact]
    public async Task Append_passes_consistency_marker_through_unchanged()
    {
        var catalog = MapCatalog.For<AuthorRegistered>("author-registered");
        var session = new EventSession(new StjEventSerializer(), catalog);
        var log = Substitute.For<IEventLog>();
        var marker = new byte[] { 9, 8, 7 };
        log.AppendAsync(
                Arg.Any<string>(),
                Arg.Any<IEnumerable<AppendEvent>>(),
                Arg.Any<long?>(),
                Arg.Any<CancellationToken>())
            .Returns(new AppendResult([4L], marker, 1));

        var adapter = new EventStoreAdapter(log, session);
        var result = await adapter.AppendAsync(
            "authors-1",
            [new AuthorRegistered(Guid.NewGuid(), DateTime.UtcNow, "Ada")]);

        Assert.Same(marker, result.ConsistencyMarker);
        Assert.Equal([4L], result.SequencePositions);
        Assert.Equal(1, result.LastStreamVersion);
    }

    [Fact]
    public async Task Registry_methods_delegate_to_the_log()
    {
        var session = new EventSession(new StjEventSerializer(), MapCatalog.For<AuthorRegistered>("author-registered"));
        var log = Substitute.For<IEventLog>();
        var metadata = new StreamMetadata { StreamId = "authors-1" };
        log.GetStreamAsync("authors-1", Arg.Any<CancellationToken>()).Returns(metadata);

        var adapter = new EventStoreAdapter(log, session);
        var actual = await adapter.GetStreamAsync("authors-1");

        Assert.Same(metadata, actual);
    }

    [EventTypeName("author-registered")]
    private sealed record AuthorRegistered(Guid Id, DateTime Timestamp, string Name) : IEvent;

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
    /// Log-only subscription handle. Writes frames; has no session.
    /// Hydrate inside this double would be a backend leak.
    /// </summary>
    private sealed class FrameHandle : IEventLogSubscriptionHandle
    {
        private readonly Channel<RecordedEvent> _channel = Channel.CreateUnbounded<RecordedEvent>();

        public FrameHandle(string subscriberId) => SubscriberId = subscriberId;

        public string SubscriberId { get; }

        public ChannelReader<RecordedEvent> Events => _channel.Reader;

        public long LastDeliveredSequence { get; private set; }

        public void Write(RecordedEvent frame)
        {
            _channel.Writer.TryWrite(frame);
            LastDeliveredSequence = frame.SequencePosition;
        }

        public void Complete() => _channel.Writer.TryComplete();

        public void Dispose() => Complete();

        public ValueTask DisposeAsync()
        {
            Complete();
            return ValueTask.CompletedTask;
        }
    }
}
