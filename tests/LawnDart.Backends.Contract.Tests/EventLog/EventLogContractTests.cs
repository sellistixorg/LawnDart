using System.Diagnostics.CodeAnalysis;
using System.Text;
using LawnDart;
using LawnDart.EventSourcing.Serialization;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Log checks that do not need a .NET event type, plus the typed adapter on top of that log.
/// </summary>
public abstract class EventLogContractTests : ContractSuite
{
    public const string OutsideEventName = "log-contract.outside-event";

    protected EventLogContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task Thesis_HostWithoutClrTypes_CanReadFilterAndCopy()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Log;
        var payload = Encoding.UTF8.GetBytes("""{"kind":"foreign","n":1}""");
        var metadata = Encoding.UTF8.GetBytes("""{"UserId":"log-only"}""");
        var streamId = Unique("thesis");
        var copyStreamId = Unique("thesis-copy");

        await log.AppendAsync(
            streamId,
            [new AppendEvent(OutsideEventName, payload, metadata, schemaVersion: 1, tags: ["order:thesis"])]);

        var recorded = Assert.Single(await log.ReadStreamAsync(streamId));
        Assert.Equal(OutsideEventName, recorded.EventType);
        Assert.Equal(payload, recorded.Payload.ToArray());
        Assert.Equal(metadata, recorded.Metadata.ToArray());
        Assert.Equal(1, recorded.SchemaVersion);
        Assert.Contains("order:thesis", recorded.Tags);

        var byToken = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByType(OutsideEventName)));
        Assert.Contains(byToken.Events, e => e.SequencePosition == recorded.SequencePosition);

        var byTag = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags("order:thesis")));
        Assert.Contains(byTag.Events, e => e.SequencePosition == recorded.SequencePosition);

        await log.AppendAsync(
            copyStreamId,
            [new AppendEvent(
                recorded.EventType,
                recorded.Payload,
                recorded.Metadata,
                recorded.SchemaVersion,
                recorded.CodecId,
                recorded.Tags)]);

        var copy = Assert.Single(await log.ReadStreamAsync(copyStreamId));
        Assert.Equal(recorded.EventType, copy.EventType);
        Assert.Equal(recorded.Payload.ToArray(), copy.Payload.ToArray());
        Assert.Equal(recorded.Metadata.ToArray(), copy.Metadata.ToArray());
        Assert.Equal(recorded.Tags, copy.Tags);
    }

    [Fact]
    public async Task AppendRead_SamePayloadWithoutHydrate()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Log;
        var payload = Encoding.UTF8.GetBytes("""{"value":"round-trip"}""");
        var streamId = Unique("payload");

        await log.AppendAsync(streamId, [new AppendEvent(OutsideEventName, payload)]);

        var recorded = Assert.Single(await log.ReadStreamAsync(streamId));
        Assert.Equal(payload, recorded.Payload.ToArray());
        Assert.Equal(OutsideEventName, recorded.EventType);
    }

    [Fact]
    public async Task Query_TokenAndTags_WithoutHydrate()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Log;
        var streamId = Unique("query");
        await log.AppendAsync(
            streamId,
            [
                new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("1"), tags: ["keep"]),
                new AppendEvent("log-contract.other-event", Encoding.UTF8.GetBytes("2"), tags: ["drop"]),
                new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("3"), tags: ["keep"])
            ]);

        var byToken = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByType(OutsideEventName)));
        Assert.Equal(2, byToken.Events.Count(e => e.StreamId == streamId));
        Assert.All(byToken.Events.Where(e => e.StreamId == streamId), e => Assert.Equal(OutsideEventName, e.EventType));

        var byTag = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags("keep")));
        Assert.Equal(2, byTag.Events.Count(e => e.StreamId == streamId));
        Assert.All(byTag.Events.Where(e => e.StreamId == streamId), e => Assert.Contains("keep", e.Tags));

        var max = await log.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags("keep")));
        Assert.Equal(byTag.Events.Max(e => e.SequencePosition), max);
    }

    [Fact]
    public async Task TypedAdapter_RegisteredType_StillRoundTrips()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Log;
        var catalog = MapCatalog.For<ContractTick>("log-contract.tick");
        var adapter = new EventStoreAdapter(log, new EventSession(new JsonEventSerializer(), catalog));
        var streamId = Unique("typed");
        var tick = new ContractTick(Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"), new DateTime(2026, 4, 5, 6, 7, 8, DateTimeKind.Utc), "Ada");

        await adapter.AppendAsync(streamId, [tick]);
        var sequenced = Assert.Single(await adapter.ReadStreamAsync(streamId));
        var read = Assert.IsType<ContractTick>(sequenced.Event);
        Assert.Equal(tick.Id, read.Id);
        Assert.Equal(tick.Name, read.Name);
        Assert.False(ReferenceEquals(tick, read));
    }

    [Fact]
    public async Task TypedAdapter_UnregisteredEventName_Throws()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Log;
        var streamId = Unique("unknown");
        await log.AppendAsync(streamId, [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"))]);

        var recorded = Assert.Single(await log.ReadStreamAsync(streamId));
        Assert.Equal(OutsideEventName, recorded.EventType);

        var adapter = new EventStoreAdapter(
            log,
            new EventSession(new JsonEventSerializer(), MapCatalog.For<ContractTick>("log-contract.tick")));
        var ex = await Assert.ThrowsAsync<UnknownEventFamilyException>(() => adapter.ReadStreamAsync(streamId));
        Assert.Contains(OutsideEventName, ex.Message);
    }

    [Fact]
    public async Task TypedAdapter_StoredPayloadFormatDiffers_Throws()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Log;
        var streamId = Unique("ctype");
        await log.AppendAsync(
            streamId,
            [new AppendEvent(
                "log-contract.tick",
                Encoding.UTF8.GetBytes("{}"),
                codecId: EventCodec.MemoryPack)]);

        var adapter = new EventStoreAdapter(
            log,
            new EventSession(new JsonEventSerializer(), MapCatalog.For<ContractTick>("log-contract.tick")));
        var ex = await Assert.ThrowsAsync<EventContentTypeMismatchException>(() => adapter.ReadStreamAsync(streamId));
        Assert.Equal(EventCodec.MemoryPack, ex.StoredCodecId);
        Assert.Equal(EventCodec.Json, ex.SessionCodecId);
        Assert.Contains(EventCodec.MemoryPackMime, ex.Message);
        Assert.DoesNotContain("migration tool", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Append_CodecId2_ReadsCodecId2()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Log;
        var streamId = Unique("codec2");
        await log.AppendAsync(
            streamId,
            [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), codecId: EventCodec.MemoryPack)]);

        var recorded = Assert.Single(await log.ReadStreamAsync(streamId));
        Assert.Equal(EventCodec.MemoryPack, recorded.CodecId);
    }

    [Fact]
    public async Task TagQuery_IsOrdinalCaseSensitive()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Log;
        var streamId = Unique("tag-case");
        var upper = "Customer:ABC:" + Guid.NewGuid().ToString("N");
        var lower = upper.ToLowerInvariant();
        await log.AppendAsync(
            streamId,
            [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), tags: [upper])]);

        var exact = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(upper)));
        Assert.Contains(exact.Events, e => e.StreamId == streamId);
        Assert.True(exact.TypesResolved);

        var otherCase = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(lower)));
        Assert.DoesNotContain(otherCase.Events, e => e.StreamId == streamId);

        var bothId = Unique("tag-both");
        await log.AppendAsync(
            bothId,
            [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), tags: [upper, lower])]);
        var byUpper = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(upper)));
        var byLower = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(lower)));
        Assert.Contains(byUpper.Events, e => e.StreamId == bothId);
        Assert.Contains(byLower.Events, e => e.StreamId == bothId);
    }

    [Fact]
    public async Task Subscription_TagQuery_IsOrdinalCaseSensitive()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var streamId = Unique("tag-sub");
        var upper = "Customer:ABC:" + Guid.NewGuid().ToString("N");
        var lower = upper.ToLowerInvariant();
        var appended = await opened.Log.AppendAsync(
            streamId,
            [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), tags: [upper])]);
        var sequence = appended.SequencePositions[0];

        if (opened.Store is not IEventLogSubscriptions subscriptions)
            throw new InvalidOperationException("Store does not subscribe.");

        var exact = Query.FromItems(QueryItem.ByTags(upper));
        await using (var handle = subscriptions.Subscribe(
            "tag-case-exact-" + Guid.NewGuid().ToString("N"),
            sequence,
            EventSubscriptionFilter.ForQuery(exact)))
        {
            var received = await ReadRecordedAsync(handle, 1, TimeSpan.FromSeconds(10));
            Assert.Equal(sequence, Assert.Single(received).SequencePosition);
        }

        var other = Query.FromItems(QueryItem.ByTags(lower));
        await using (var handle = subscriptions.Subscribe(
            "tag-case-other-" + Guid.NewGuid().ToString("N"),
            sequence,
            EventSubscriptionFilter.ForQuery(other)))
        {
            var received = await DrainRecordedAsync(handle, TimeSpan.FromMilliseconds(400));
            Assert.DoesNotContain(received, e => e.SequencePosition == sequence);
        }
    }

    [Fact]
    public async Task Append_SameEventId_FailsAsAlreadyWritten()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Log;
        var eventId = Guid.NewGuid();
        var metadata = Encoding.UTF8.GetBytes($$"""{"EventId":"{{eventId}}"}""");
        var first = Unique("event-id");
        await log.AppendAsync(first, [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), metadata)]);

        var ex = await Assert.ThrowsAsync<EventAlreadyAppendedException>(() =>
            log.AppendAsync(Unique("event-id-replay"), [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), metadata)]));
        Assert.Equal(eventId, ex.EventId);

        var stored = await log.ReadStreamAsync(first);
        Assert.Single(stored);
    }

    [ContractFact]
    public async Task Append_SameEventId_OnAnotherTenant_FollowsTheBackend()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Log;
        var eventId = Guid.NewGuid();
        var metadata = Encoding.UTF8.GetBytes($$"""{"EventId":"{{eventId}}"}""");
        var first = "alpha:Order:" + Guid.NewGuid().ToString("N");
        var second = "beta:Order:" + Guid.NewGuid().ToString("N");
        await log.AppendAsync(first, [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), metadata)]);

        if (Cap.EnforcesEventIdAcrossTenants)
        {
            var ex = await Assert.ThrowsAsync<EventAlreadyAppendedException>(() =>
                log.AppendAsync(second, [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), metadata)]));
            Assert.Equal(eventId, ex.EventId);
            Assert.Empty(await log.ReadStreamAsync(second));
        }
        else
        {
            await log.AppendAsync(second, [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), metadata)]);
            Assert.Single(await log.ReadStreamAsync(second));
        }
    }

    private static string Unique(string suffix) => $"log-contract:{suffix}:{Guid.NewGuid():N}";

    private sealed class MapCatalog : IEventTypeCatalog
    {
        private readonly Dictionary<Type, string> _typeToToken = new();
        private readonly Dictionary<string, Type> _tokenToType = new(StringComparer.Ordinal);

        public static MapCatalog For<TEvent>(string token) where TEvent : IEvent
        {
            var catalog = new MapCatalog();
            catalog._typeToToken[typeof(TEvent)] = token;
            catalog._tokenToType[token] = typeof(TEvent);
            return catalog;
        }

        public string GetName(Type type) => _typeToToken[type];

        public bool TryResolveType(string storedName, [NotNullWhen(true)] out Type? type)
            => _tokenToType.TryGetValue(storedName, out type);
    }
}
