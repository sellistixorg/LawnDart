using System.Diagnostics.CodeAnalysis;
using System.Text;
using LawnDart;
using LawnDart.EventSourcing.Serialization;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.EventSourcing.Testing;

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
    public async Task HostWithoutEventClasses_CanReadFilterAndCopy()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Log;
        var payload = Encoding.UTF8.GetBytes("""{"kind":"foreign","n":1}""");
        var metadata = Encoding.UTF8.GetBytes("""{"UserId":"log-only"}""");
        var streamId = Unique("no-clr");
        var copyStreamId = Unique("no-clr-copy");

        await log.AppendAsync(
            StreamId.Parse(streamId),
            [new AppendEvent(OutsideEventName, payload, metadata, schemaVersion: 1, tags: [Tag.Parse("order:outside")])]);

        var recorded = Assert.Single(await log.ReadStreamAsync(StreamId.Parse(streamId)));
        Assert.Equal(OutsideEventName, recorded.EventType);
        AssertStoredPayload(payload, recorded.Payload, recorded.CodecId);
        AssertStoredMetadata(metadata, recorded.Metadata);
        Assert.Equal(1, recorded.SchemaVersion);
        Assert.Contains("order:outside", recorded.Tags.Select(static t => t.ToString()).ToArray());

        var byToken = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByType(OutsideEventName)).ForTenant("log-contract"));
        Assert.Contains(byToken.Events, e => e.SequencePosition == recorded.SequencePosition);

        var byTag = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(Tag.Parse("order:outside"))).ForTenant("log-contract"));
        Assert.Contains(byTag.Events, e => e.SequencePosition == recorded.SequencePosition);

        await log.AppendAsync(
            StreamId.Parse(copyStreamId),
            [new AppendEvent(
                recorded.EventType,
                recorded.Payload,
                recorded.Metadata,
                recorded.SchemaVersion,
                recorded.CodecId,
                recorded.Tags)]);

        var copy = Assert.Single(await log.ReadStreamAsync(StreamId.Parse(copyStreamId)));
        Assert.Equal(recorded.EventType, copy.EventType);
        AssertStoredPayload(recorded.Payload.ToArray(), copy.Payload, copy.CodecId);
        AssertStoredMetadata(recorded.Metadata.ToArray(), copy.Metadata);
        Assert.Equal(recorded.Tags, copy.Tags);
    }

    [Fact]
    public async Task AppendRead_SamePayloadWithoutHydrate()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Log;
        var payload = Encoding.UTF8.GetBytes("""{"value":"round-trip"}""");
        var streamId = Unique("payload");

        await log.AppendAsync(StreamId.Parse(streamId), [new AppendEvent(OutsideEventName, payload)]);

        var recorded = Assert.Single(await log.ReadStreamAsync(StreamId.Parse(streamId)));
        AssertStoredPayload(payload, recorded.Payload, recorded.CodecId);
        Assert.Equal(OutsideEventName, recorded.EventType);
    }

    [Fact]
    public async Task Query_TokenAndTags_WithoutHydrate()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Log;
        var streamId = Unique("query");
        await log.AppendAsync(
            StreamId.Parse(streamId),
            [
                new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("1"), tags: [Tag.Parse("tag:keep")]),
                new AppendEvent("log-contract.other-event", Encoding.UTF8.GetBytes("2"), tags: [Tag.Parse("tag:drop")]),
                new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("3"), tags: [Tag.Parse("tag:keep")])
            ]);

        var byToken = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByType(OutsideEventName)).ForTenant("log-contract"));
        Assert.Equal(2, byToken.Events.Count(e => e.StreamId.ToString() == streamId));
        Assert.All(byToken.Events.Where(e => e.StreamId.ToString() == streamId), e => Assert.Equal(OutsideEventName, e.EventType));

        var byTag = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(Tag.Parse("tag:keep"))).ForTenant("log-contract"));
        Assert.Equal(2, byTag.Events.Count(e => e.StreamId.ToString() == streamId));
        Assert.All(byTag.Events.Where(e => e.StreamId.ToString() == streamId), e => Assert.Contains(Tag.Parse("tag:keep"), e.Tags));

        var max = await log.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags(Tag.Parse("tag:keep"))).ForTenant("log-contract"));
        Assert.Equal(byTag.Events.Max(e => e.SequencePosition), max);
    }

    [Fact]
    public async Task TypedAdapter_RegisteredType_StillRoundTrips()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Log;
        var catalog = MapCatalog.For<ContractTick>("log-contract.tick");
        var adapter = new EventStoreAdapter(log, new EventSession(new JsonEventSerializer(), catalog));
        var streamId = Unique("typed");
        var tick = new ContractTick(Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"), new DateTime(2026, 4, 5, 6, 7, 8, DateTimeKind.Utc), "Ada");

        await adapter.AppendAsync(StreamId.Parse(streamId), [tick]);
        var sequenced = Assert.Single(await adapter.ReadStreamAsync(StreamId.Parse(streamId)));
        var read = Assert.IsType<ContractTick>(sequenced.Event);
        Assert.Equal(tick.Id, read.Id);
        Assert.Equal(tick.Name, read.Name);
        Assert.False(ReferenceEquals(tick, read));
    }

    [Fact]
    public async Task TypedAdapter_UnregisteredEventName_Throws()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Log;
        var streamId = Unique("unknown");
        await log.AppendAsync(StreamId.Parse(streamId), [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"))]);

        var recorded = Assert.Single(await log.ReadStreamAsync(StreamId.Parse(streamId)));
        Assert.Equal(OutsideEventName, recorded.EventType);

        var adapter = new EventStoreAdapter(
            log,
            new EventSession(new JsonEventSerializer(), MapCatalog.For<ContractTick>("log-contract.tick")));
        var ex = await Assert.ThrowsAsync<UnknownEventFamilyException>(() => adapter.ReadStreamAsync(StreamId.Parse(streamId)));
        Assert.Contains(OutsideEventName, ex.Message);
    }

    [Fact]
    public async Task TypedAdapter_StoredPayloadFormatDiffers_Throws()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Log;
        var streamId = Unique("ctype");
        await log.AppendAsync(
            StreamId.Parse(streamId),
            [new AppendEvent(
                "log-contract.tick",
                Encoding.UTF8.GetBytes("{}"),
                codecId: EventCodec.MemoryPack)]);

        var adapter = new EventStoreAdapter(
            log,
            new EventSession(new JsonEventSerializer(), MapCatalog.For<ContractTick>("log-contract.tick")));
        var ex = await Assert.ThrowsAsync<EventContentTypeMismatchException>(() => adapter.ReadStreamAsync(StreamId.Parse(streamId)));
        Assert.Equal(EventCodec.MemoryPack, ex.StoredCodecId);
        Assert.Equal(EventCodec.Json, ex.SessionCodecId);
        Assert.Contains(EventCodec.MemoryPackMime, ex.Message);
        Assert.DoesNotContain("migration tool", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Append_CodecId2_ReadsCodecId2()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Log;
        var streamId = Unique("codec2");
        await log.AppendAsync(
            StreamId.Parse(streamId),
            [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), codecId: EventCodec.MemoryPack)]);

        var recorded = Assert.Single(await log.ReadStreamAsync(StreamId.Parse(streamId)));
        Assert.Equal(EventCodec.MemoryPack, recorded.CodecId);
    }

    [Fact]
    public async Task TagQuery_IsOrdinalCaseSensitive()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Log;
        var streamId = Unique("tag-case");
        var upper = "Customer:ABC:" + Guid.NewGuid().ToString("N");
        var lower = upper.ToLowerInvariant();
        await log.AppendAsync(
            StreamId.Parse(streamId),
            [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), tags: [Tag.Parse(upper)])]);

        var exact = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(Tag.Parse(upper))).ForTenant("log-contract"));
        Assert.Contains(exact.Events, e => e.StreamId.ToString() == streamId);
        Assert.True(exact.TypesResolved);

        var otherCase = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(Tag.Parse(lower))).ForTenant("log-contract"));
        Assert.DoesNotContain(otherCase.Events, e => e.StreamId.ToString() == streamId);

        var bothId = Unique("tag-both");
        await log.AppendAsync(
            StreamId.Parse(bothId),
            [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), tags: [Tag.Parse(upper), Tag.Parse(lower)])]);
        var byUpper = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(Tag.Parse(upper))).ForTenant("log-contract"));
        var byLower = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(Tag.Parse(lower))).ForTenant("log-contract"));
        Assert.Contains(byUpper.Events, e => e.StreamId.ToString() == bothId);
        Assert.Contains(byLower.Events, e => e.StreamId.ToString() == bothId);
    }

    [Fact]
    public async Task Subscription_TagQuery_IsOrdinalCaseSensitive()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared));
        var streamId = Unique("tag-sub");
        var upper = "Customer:ABC:" + Guid.NewGuid().ToString("N");
        var lower = upper.ToLowerInvariant();
        var appended = await opened.Log.AppendAsync(
            StreamId.Parse(streamId),
            [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), tags: [Tag.Parse(upper)])]);
        var sequence = appended.SequencePositions[0];

        if (opened.Store is not IEventLogSubscriptions subscriptions)
            throw new InvalidOperationException("Store does not subscribe.");

        var exact = Query.FromItems(QueryItem.ByTags(Tag.Parse(upper))).ForTenant("log-contract");
        await using (var handle = subscriptions.Subscribe(
            "tag-case-exact-" + Guid.NewGuid().ToString("N"),
            sequence,
            EventSubscriptionFilter.ForQuery(exact)))
        {
            var received = await ReadRecordedAsync(handle, 1, TimeSpan.FromSeconds(10));
            Assert.Equal(sequence, Assert.Single(received).SequencePosition);
        }

        var other = Query.FromItems(QueryItem.ByTags(Tag.Parse(lower))).ForTenant("log-contract");
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
    public async Task Append_SameEventId_FollowsTheBackend()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Log;
        var eventId = Guid.NewGuid();
        var metadata = Encoding.UTF8.GetBytes($$"""{"EventId":"{{eventId}}"}""");
        var first = Unique("event-id");
        var second = Unique("event-id-replay");
        await log.AppendAsync(StreamId.Parse(first), [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), metadata)]);

        if (!Cap.AllowsDuplicateEventIds)
        {
            var ex = await Assert.ThrowsAsync<EventAlreadyAppendedException>(() =>
                log.AppendAsync(StreamId.Parse(second), [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), metadata)]));
            Assert.Equal(eventId, ex.EventId);
            Assert.Empty(await log.ReadStreamAsync(StreamId.Parse(second)));
        }
        else
        {
            // Event ids are not unique. The stream version check and the append
            // condition guard retries, not the event id.
            await log.AppendAsync(StreamId.Parse(second), [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), metadata)]);
            Assert.Single(await log.ReadStreamAsync(StreamId.Parse(second)));
        }

        var stored = await log.ReadStreamAsync(StreamId.Parse(first));
        Assert.Single(stored);
    }

    [ContractFact]
    public async Task Append_SameEventId_OnAnotherTenant_FollowsTheBackend()
    {
        var log = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Log;
        var eventId = Guid.NewGuid();
        var metadata = Encoding.UTF8.GetBytes($$"""{"EventId":"{{eventId}}"}""");
        var first = "alpha:Order:" + Guid.NewGuid().ToString("N");
        var second = "beta:Order:" + Guid.NewGuid().ToString("N");
        await log.AppendAsync(StreamId.Parse(first), [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), metadata)]);

        if (Cap.EnforcesEventIdAcrossTenants)
        {
            var ex = await Assert.ThrowsAsync<EventAlreadyAppendedException>(() =>
                log.AppendAsync(StreamId.Parse(second), [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), metadata)]));
            Assert.Equal(eventId, ex.EventId);
            Assert.Empty(await log.ReadStreamAsync(StreamId.Parse(second)));
        }
        else
        {
            await log.AppendAsync(StreamId.Parse(second), [new AppendEvent(OutsideEventName, Encoding.UTF8.GetBytes("{}"), metadata)]);
            Assert.Single(await log.ReadStreamAsync(StreamId.Parse(second)));
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
