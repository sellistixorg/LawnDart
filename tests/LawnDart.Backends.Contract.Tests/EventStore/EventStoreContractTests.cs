using System.Text;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Stream appends, reads, versions, metadata, and tenant checks.
/// </summary>
public abstract class EventStoreContractTests : ContractSuite
{
    protected EventStoreContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task ReadStreamAsync_EmptyStream_ReturnsEmpty()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        Assert.Empty(await store.ReadStreamAsync(Stream()));
    }

    [Fact]
    public async Task AppendAsync_ThenReadStream_ReturnsEvents()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = Stream();
        await store.AppendAsync(streamId, [ContractEvent.Tick(), ContractEvent.Tick()]);
        var result = await store.ReadStreamAsync(streamId);
        Assert.Equal(2, result.Count);
        Assert.Equal(1, result[0].Version);
        Assert.Equal(2, result[1].Version);
    }

    [Fact]
    public async Task AppendAsync_WithExpectedVersion_ValidatesConcurrency()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = Stream();
        await store.AppendAsync(streamId, [ContractEvent.Tick()]);
        await store.AppendAsync(streamId, [ContractEvent.Tick()], expectedVersion: 1);
        Assert.Equal(2, (await store.ReadStreamAsync(streamId)).Count);
    }

    [Fact]
    public async Task AppendAsync_WithWrongExpectedVersion_ThrowsConcurrencyException()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = Stream();
        await store.AppendAsync(streamId, [ContractEvent.Tick()]);
        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            store.AppendAsync(streamId, [ContractEvent.Tick()], expectedVersion: 5));
    }

    [ContractFact]
    public async Task AppendAsync_ParallelAppendsToOneStream_DuplicateKeyIsConcurrencyException()
    {
        SkipUnless(
            Cap.ParallelAppendCanConflict,
            "This backend runs one append to a stream at a time, so a parallel append does not hit a duplicate version.");

        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = Stream();
        ConcurrencyException? duplicateKey = null;

        for (var round = 0; round < 25 && duplicateKey is null; round++)
        {
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var tasks = Enumerable.Range(0, 12).Select(_ => Task.Run(async () =>
            {
                await start.Task.ConfigureAwait(false);
                try
                {
                    await store.AppendAsync(streamId, [ContractEvent.Tick()]);
                    return (Exception?)null;
                }
                catch (Exception ex)
                {
                    return ex;
                }
            })).ToArray();

            start.SetResult();
            var results = await Task.WhenAll(tasks);
            Assert.Contains(results, ex => ex is null);
            foreach (var ex in results)
            {
                if (ex is null)
                    continue;
                var concurrency = Assert.IsType<ConcurrencyException>(ex);
                Assert.NotNull(concurrency.ExpectedPosition);
                Assert.NotNull(concurrency.ActualPosition);
                Assert.True(concurrency.ActualPosition >= concurrency.ExpectedPosition);
                if (Cap.DuplicateStreamVersionIncludesDatabaseError
                    && concurrency.InnerException is SqlException sql
                    && sql.Number is 2627 or 2601)
                    duplicateKey ??= concurrency;
                else if (!Cap.DuplicateStreamVersionIncludesDatabaseError)
                    duplicateKey ??= concurrency;
            }
        }

        Assert.NotNull(duplicateKey);
        Assert.True(duplicateKey.ActualPosition > duplicateKey.ExpectedPosition);
        var stored = await store.ReadStreamAsync(streamId);
        Assert.NotEmpty(stored);
        for (var i = 0; i < stored.Count; i++)
            Assert.Equal(i + 1, stored[i].Version);
    }

    [Fact]
    public async Task AppendAsync_PropagatesMetadata()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = "tenant456:TestAggregate:" + Guid.NewGuid().ToString("N");
        var metadata = new EventMetadata
        {
            UserId = "user123",
            TenantId = "tenant456",
            CorrelationId = "corr-123"
        };
        await store.AppendAsync(streamId, [ContractEvent.Tick()], metadata: metadata);
        var result = Assert.Single(await store.ReadStreamAsync(streamId));
        Assert.Equal("user123", result.Metadata.UserId);
        Assert.Equal("tenant456", result.Metadata.TenantId);
        Assert.Equal("corr-123", result.Metadata.CorrelationId);
    }

    [Fact]
    public async Task AppendAsync_WithTags_StoresTags()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = Stream();
        await store.AppendAsync(streamId, [ContractEvent.Tick()], tags: ["alpha", "beta"]);
        var result = Assert.Single(await store.ReadStreamAsync(streamId));
        Assert.Contains("alpha", result.Tags);
        Assert.Contains("beta", result.Tags);
    }

    [Fact]
    public async Task ReadStreamAsync_WithToVersion_LimitsResults()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = Stream();
        await store.AppendAsync(streamId, [ContractEvent.Tick(), ContractEvent.Tick(), ContractEvent.Tick(), ContractEvent.Tick()]);
        var result = await store.ReadStreamAsync(streamId, toVersion: 2);
        Assert.Equal(2, result.Count);
        Assert.Equal(1, result[0].Version);
        Assert.Equal(2, result[1].Version);
    }

    [Fact]
    public async Task ReadStreamEnumerableAsync_YieldsEventsInOrder()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = Stream();
        await store.AppendAsync(streamId, [ContractEvent.Tick(), ContractEvent.Tick(), ContractEvent.Tick()]);
        var versions = new List<long>();
        await foreach (var sequenced in store.ReadStreamEnumerableAsync(streamId))
            versions.Add(sequenced.Version);
        Assert.Equal(3, versions.Count);
        Assert.Equal(1, versions[0]);
        Assert.Equal(2, versions[1]);
        Assert.Equal(3, versions[2]);
    }

    [Fact]
    public async Task ReadStreamEnumerableAsync_WithToVersion_RespectsLimit()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = Stream();
        await store.AppendAsync(streamId, [ContractEvent.Tick(), ContractEvent.Tick(), ContractEvent.Tick()]);
        var count = 0;
        await foreach (var _ in store.ReadStreamEnumerableAsync(streamId, toVersion: 2))
            count++;
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task ReadStreamAsync_WithToTimestamp_LimitsResults()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = Stream();
        var early = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var late = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        await store.AppendAsync(streamId, [new ContractTestEvent(Guid.NewGuid(), early)], metadata: new EventMetadata { Timestamp = early });
        await store.AppendAsync(streamId, [new ContractTestEvent(Guid.NewGuid(), late)], metadata: new EventMetadata { Timestamp = late });
        var result = await store.ReadStreamAsync(streamId, toTimestamp: early);
        Assert.Single(result);
        Assert.Equal(early, result[0].Metadata.Timestamp);
    }

    [Fact]
    public async Task ReadByQueryAsync_WithTagFilter_ReturnsMatchingEvents()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var tag = "order:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [tag]);
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: ["other"]);
        var result = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tag)));
        Assert.Single(result.Events);
        Assert.Contains(tag, result.Events[0].Tags);
    }

    [Fact]
    public async Task ReadByQueryAsync_WithTypeFilter_ReturnsMatchingEvents()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = Stream();
        await store.AppendAsync(streamId, [ContractEvent.Tick(), new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        var name = EventTypeCatalog.TryGetDeclaredName(typeof(ContractTestEvent))!;
        var result = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByType(name)));
        var matched = Assert.Single(result.Events);
        Assert.IsType<ContractTestEvent>(matched.Event);
    }

    [Fact]
    public async Task AppendAsync_ThenReadStream_PreservesEventProperties()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = Stream();
        var original = new ContractEventWithProperties(Guid.NewGuid(), DateTime.UtcNow, "product-123", 5);
        await store.AppendAsync(streamId, [original]);
        var read = Assert.IsType<ContractEventWithProperties>(Assert.Single(await store.ReadStreamAsync(streamId)).Event);
        Assert.Equal(original.Id, read.Id);
        Assert.Equal(original.Timestamp, read.Timestamp);
        Assert.Equal(original.ProductId, read.ProductId);
        Assert.Equal(original.Quantity, read.Quantity);
    }

    [Fact]
    public async Task ReadByQueryAsync_WithAliasedTypeFilter_UsesEventTypeAlias()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = Stream();
        await store.AppendAsync(streamId, [new ContractAliasedEvent(Guid.NewGuid(), DateTime.UtcNow), ContractEvent.Tick()]);
        var name = EventTypeCatalog.TryGetDeclaredName(typeof(ContractAliasedEvent))!;
        var result = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByType(name)));
        Assert.IsType<ContractAliasedEvent>(Assert.Single(result.Events).Event);
    }

    [Fact]
    public async Task ReadByQueryAsync_WithAndTags_ReturnsIntersection()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var order = "order:" + Guid.NewGuid().ToString("N");
        var extra = "extra:" + Guid.NewGuid().ToString("N");
        var only = "only:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [order, only]);
        var match = await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [order, extra]);
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [extra]);
        var result = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(order, extra)));
        var evt = Assert.Single(result.Events);
        Assert.Equal(match.SequencePositions[0], evt.SequencePosition);
        Assert.Contains(order, evt.Tags);
        Assert.Contains(extra, evt.Tags);
    }

    [Fact]
    public async Task ReadByQueryAsync_WithOrTagItems_ReturnsUnion()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var left = "left:" + Guid.NewGuid().ToString("N");
        var right = "right:" + Guid.NewGuid().ToString("N");
        var decoy = "decoy:" + Guid.NewGuid().ToString("N");
        var a = await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [left]);
        var b = await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [right]);
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [decoy]);
        var result = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(left), QueryItem.ByTags(right)));
        Assert.Equal(2, result.Events.Count);
        Assert.Equal(
            new[] { a.SequencePositions[0], b.SequencePositions[0] },
            result.Events.Select(e => e.SequencePosition).ToArray());
    }

    [Fact]
    public async Task GetMaxSequencePositionAsync_NoMatches_ReturnsZero()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: ["present"]);
        Assert.Equal(0, await store.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags("missing-" + Guid.NewGuid().ToString("N")))));
    }

    [Fact]
    public async Task GetMaxSequencePositionAsync_WithTag_ReturnsMaxWithoutFullReplay()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var tag = "max:" + Guid.NewGuid().ToString("N");
        var other = "max-other:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [other]);
        var first = await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [tag]);
        var last = await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [tag]);
        var query = Query.FromItems(QueryItem.ByTags(tag));
        var max = await store.GetMaxSequencePositionAsync(query);
        Assert.Equal(last.SequencePositions[0], max);
        Assert.NotEqual(first.SequencePositions[0], max);
        Assert.Equal(max, await store.GetMaxSequencePositionAsync(query, fromSequencePosition: first.SequencePositions[0]));
        Assert.Equal(0L, await store.GetMaxSequencePositionAsync(query, fromSequencePosition: last.SequencePositions[0] + 1));
    }

    [Fact]
    public async Task GetMaxSequencePositionAsync_QueryAll_MatchesStoreHead()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await store.AppendAsync(Stream(), [ContractEvent.Tick(), ContractEvent.Tick()]);
        Assert.Equal(await store.GetCurrentSequenceAsync(), await store.GetMaxSequencePositionAsync(Query.All()));
    }

    [Fact]
    public async Task Log_can_materialize_recorded_event_without_registered_clr_type()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var streamId = Stream();
        const string token = "audit-probe.outside-event";
        var payload = Encoding.UTF8.GetBytes("""{"kind":"opaque"}""");
        var metadata = Encoding.UTF8.GetBytes("""{"UserId":"log"}""");
        await opened.Log.AppendAsync(streamId, [new AppendEvent(token, payload, metadata, schemaVersion: 1)]);
        var recorded = Assert.Single(await opened.Log.ReadStreamAsync(streamId));
        Assert.Equal(token, recorded.EventType);
        Assert.Equal(1, recorded.SchemaVersion);
        Assert.Equal(EventCodec.Json, recorded.CodecId);
        AssertStoredPayload(payload, recorded.Payload, recorded.CodecId);
        AssertStoredMetadata(metadata, recorded.Metadata);
        var ex = await Assert.ThrowsAsync<UnknownEventFamilyException>(() => opened.Store.ReadStreamAsync(streamId));
        Assert.Contains(token, ex.Message);
    }

    [Fact]
    public async Task ReadByQuery_KnownTypeBesideAnUnknownName_ReturnsTheKnownEvents()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var streamId = Stream();
        await opened.Store.AppendAsync(streamId, [ContractEvent.Tick()]);
        var known = EventTypeCatalog.TryGetDeclaredName(typeof(ContractTestEvent))!;
        var query = Query.FromItems(QueryItem.ByType(known, "no.such.event." + Guid.NewGuid().ToString("N")));
        var read = await opened.Store.ReadByQueryAsync(query);
        var sequenced = Assert.Single(read.Events);
        Assert.IsType<ContractTestEvent>(sequenced.Event);
        if (Cap.UnknownEventNameMarksQueryUnresolved)
            Assert.False(read.TypesResolved);
        else
            Assert.True(read.TypesResolved);
    }

    [ContractFact]
    public async Task SchemaInit_RecordsCatalogEventNames_AndASecondCallIsHarmless()
    {
        SkipUnless(
            Cap.UnknownEventNameMarksQueryUnresolved,
            "This store has no event-name table.");

        var storedName = EventTypeCatalog.TryGetDeclaredName(typeof(ContractTestEvent))!;
        var otherName = EventTypeCatalog.TryGetDeclaredName(typeof(ContractAnotherEvent))!;
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(
            eventTypes: [typeof(ContractTestEvent), typeof(ContractAnotherEvent)]));

        Assert.Equal(1, await CountEventNamesAsync(opened, storedName));
        Assert.Equal(1, await CountEventNamesAsync(opened, otherName));

        await opened.Store.AppendAsync(Stream(), [ContractEvent.Tick()]);
        var query = Query.FromItems(QueryItem.ByType(storedName, otherName));
        var read = await opened.Store.ReadByQueryAsync(query);
        var sequenced = Assert.Single(read.Events);
        Assert.IsType<ContractTestEvent>(sequenced.Event);
        Assert.True(read.TypesResolved);

        await InitializeSchemaAsync(opened);
        await Task.WhenAll(InitializeSchemaAsync(opened), InitializeSchemaAsync(opened));

        Assert.Equal(1, await CountEventNamesAsync(opened, storedName));
        Assert.Equal(1, await CountEventNamesAsync(opened, otherName));
        var again = await opened.Store.ReadByQueryAsync(query);
        Assert.Single(again.Events);
        Assert.True(again.TypesResolved);
    }

    [ContractFact]
    public async Task ReadByQuery_NameStoredDuringTheRead_DoesNotSkipTheEarlierEvent()
    {
        SkipUnless(
            Cap.UnknownEventNameMarksQueryUnresolved,
            "This store has no event-name table.");

        var schema = "c" + Guid.NewGuid().ToString("N");
        var options = new ContractStoreOptions { SchemaName = schema, RequireTenantId = false };
        var writer = await Backend.OpenEventStoreAsync(options);
        var reader = await Backend.OpenEventStoreAsync(options);
        var reloadGate = new ReloadGate();
        if (reader.Store is PostgresEventStore postgres)
        {
            // A pooled connection reopens too quickly for the writer to land
            // between the name reload and the read limit. Wait here, after the
            // reload has released its lock, until this round's writes commit.
            postgres.AfterEventNameReload = _ => reloadGate.Current?.Task ?? Task.CompletedTask;
        }

        const string known = "race.known";
        await writer.Log.AppendAsync(Stream(), [new AppendEvent(known, "{}"u8.ToArray())]);
        var primed = await reader.Log.ReadByQueryAsync(Query.FromItems(QueryItem.ByType(known)));
        var cursor = Assert.Single(primed.Events).SequencePosition;

        // A few hundred rounds is enough to catch a read that returns the later
        // event and skips the earlier one. SQL Server needs a longer loop.
        const int rounds = 1000;
        for (var round = 0; round < rounds; round++)
        {
            var fresh = "race.new." + round.ToString("D4") + "." + Guid.NewGuid().ToString("N");
            var query = Query.FromItems(QueryItem.ByType(known, fresh));
            var readsStarted = 0;
            var writesFinished = 0;
            long freshPosition = 0;
            long knownPosition = 0;
            var seen = new Dictionary<long, string>();
            var roundWrites = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            reloadGate.Current = roundWrites;

            var reading = Task.Run(async () =>
            {
                var local = cursor;
                var deadline = 0L;
                while (deadline == 0 || Environment.TickCount64 < deadline)
                {
                    Interlocked.Increment(ref readsStarted);
                    var read = await reader.Log.ReadByQueryAsync(
                        query,
                        fromSequencePosition: local + 1);
                    foreach (var recorded in read.Events)
                    {
                        seen[recorded.SequencePosition] = recorded.EventType;
                        if (recorded.SequencePosition > local)
                            local = recorded.SequencePosition;
                    }

                    var wantFresh = Volatile.Read(ref freshPosition);
                    var wantKnown = Volatile.Read(ref knownPosition);
                    if (wantFresh != 0 && seen.ContainsKey(wantFresh) && seen.ContainsKey(wantKnown))
                        break;
                    if (Volatile.Read(ref writesFinished) == 1 && deadline == 0)
                        deadline = Environment.TickCount64 + 5000;
                }

                return local;
            });

            while (Volatile.Read(ref readsStarted) == 0)
                await Task.Yield();

            var freshAppend = await writer.Log.AppendAsync(Stream(), [new AppendEvent(fresh, "{}"u8.ToArray())]);
            var knownAppend = await writer.Log.AppendAsync(Stream(), [new AppendEvent(known, "{}"u8.ToArray())]);
            Volatile.Write(ref freshPosition, freshAppend.SequencePositions[0]);
            Volatile.Write(ref knownPosition, knownAppend.SequencePositions[0]);
            Volatile.Write(ref writesFinished, 1);
            roundWrites.TrySetResult();

            cursor = await reading;
            Assert.True(
                seen.ContainsKey(freshPosition),
                $"Round {round} returned {known} at {knownPosition} and skipped {fresh} at {freshPosition}.");
            Assert.True(seen.ContainsKey(knownPosition), $"Round {round} did not return {known} at {knownPosition}.");
            Assert.True(freshPosition < knownPosition);
        }
    }

    [ContractFact]
    public async Task UnknownType_ReturnsNoRows_AndDoesNotClaimTheRangeWasScanned()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await opened.Store.AppendAsync(Stream(), [ContractEvent.Tick()]);
        var read = await opened.Log.ReadByQueryAsync(Query.FromItems(QueryItem.ByType("no.such.event.name")));
        Assert.Empty(read.Events);
        if (Cap.UnknownEventNameMarksQueryUnresolved)
            Assert.False(read.TypesResolved);
        else
            Assert.True(read.TypesResolved);
    }

    [ContractFact]
    public async Task StaleTypeCache_RefreshesAndSeesTheNewToken()
    {
        SkipUnless(
            Cap.SecondInstanceSeesCommittedEvents,
            "In-memory keeps events in the store instance. A second instance does not see them, and there is no event-name cache to refresh.");

        var schema = "c" + Guid.NewGuid().ToString("N");
        var options = new ContractStoreOptions { SchemaName = schema, RequireTenantId = false };
        var writer = await Backend.OpenEventStoreAsync(options);
        var reader = await Backend.OpenEventStoreAsync(options);
        const string token = "audit-probe.other-event";
        await writer.Log.AppendAsync("w1", [new AppendEvent(token, "{}"u8.ToArray())]);

        var query = Query.FromItems(QueryItem.ByType(token));
        var read = await reader.Log.ReadByQueryAsync(query);
        Assert.True(read.TypesResolved);
        var recorded = Assert.Single(read.Events);
        Assert.Equal(token, recorded.EventType);

        await using var handle = ((IEventLogSubscriptions)reader.Store).Subscribe(
            "cache-sub",
            fromSequence: 1,
            EventSubscriptionFilter.ForQuery(query));
        var delivered = await ReadRecordedAsync(handle, 1, TimeSpan.FromSeconds(15));
        Assert.Equal(recorded.SequencePosition, delivered[0].SequencePosition);
    }

    [ContractFact]
    public async Task StreamId_Case_FollowsTheBackend()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var id = Guid.NewGuid().ToString("N");
        var written = $"acme:Order:{id}";
        var otherCase = $"acme:order:{id}";
        await store.AppendAsync(written, [ContractEvent.Tick()]);
        var read = await store.ReadStreamAsync(otherCase);
        if (Cap.StreamIdsCaseInsensitive)
            Assert.Single(read);
        else
            Assert.Empty(read);
    }

    [Fact]
    public async Task AppendAsync_ReservedTenantDefault_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(requireTenantId: true))).Store;
        var streamId = $"default:Order:{Guid.NewGuid()}";
        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync(streamId, [ContractEvent.Tick()], metadata: new EventMetadata { TenantId = "default" }));
        Assert.Contains("reserved", ex.Message);
    }

    [Fact]
    public async Task AppendAsync_ValidateNamesFalse_AllowsReservedTenantDefault()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(requireTenantId: true, validateNames: false))).Store;
        var streamId = $"default:Order:{Guid.NewGuid()}";
        await store.AppendAsync(streamId, [ContractEvent.Tick()], metadata: new EventMetadata { TenantId = "default" });
        Assert.Single(await store.ReadStreamAsync(streamId));
        var stream = await store.GetStreamAsync(streamId);
        Assert.NotNull(stream);
        Assert.Equal("default", stream!.TenantId);
    }

    [Fact]
    public async Task AppendAsync_EmptyTenantId_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(requireTenantId: true, validateNames: false))).Store;
        var streamId = $":Order:{Guid.NewGuid()}";
        await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(streamId, [ContractEvent.Tick()]));
    }

    [Fact]
    public async Task AppendAsync_MissingTenant_WhenRequired_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(requireTenantId: true))).Store;
        var streamId = $"Order:{Guid.NewGuid()}";
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(streamId, [ContractEvent.Tick()]));
        Assert.Contains("must include tenant ID", ex.Message);
    }

    [Fact]
    public async Task Events_AppendedToOneContext_AreNotVisibleFromAnother()
    {
        var ordering = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var catalog = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = "no-tenant:Order:order-1";
        await ordering.AppendAsync(streamId, [ContractEvent.Tick()]);
        Assert.Empty(await catalog.ReadStreamAsync(streamId));
        Assert.Single(await ordering.ReadStreamAsync(streamId));
    }

    [Fact]
    public async Task SequenceCounters_AreIndependentPerContext()
    {
        var ordering = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var catalog = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var left = await ordering.AppendAsync(Stream(), [ContractEvent.Tick()]);
        var right = await catalog.AppendAsync(Stream(), [ContractEvent.Tick()]);
        Assert.True(left.SequencePositions[0] >= 1);
        Assert.True(right.SequencePositions[0] >= 1);
        Assert.Equal(left.SequencePositions[0], await ordering.GetCurrentSequenceAsync());
        Assert.Equal(right.SequencePositions[0], await catalog.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task ReadByQueryAll_OnlyReturnsEventsFromOwnContext()
    {
        var ordering = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var catalog = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var orderStream = Stream();
        var productStream = Stream();
        await ordering.AppendAsync(orderStream, [ContractEvent.Tick()]);
        await catalog.AppendAsync(productStream, [ContractEvent.Tick()]);
        var orders = await ordering.ReadByQueryAsync(Query.All());
        var products = await catalog.ReadByQueryAsync(Query.All());
        Assert.Contains(orders.Events, e => e.StreamId == orderStream);
        Assert.DoesNotContain(orders.Events, e => e.StreamId == productStream);
        Assert.Contains(products.Events, e => e.StreamId == productStream);
        Assert.DoesNotContain(products.Events, e => e.StreamId == orderStream);
    }

    [Fact]
    public async Task MultipleEvents_SequencePositionsArePerSchema()
    {
        var ordering = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var catalog = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamA = "no-tenant:Order:" + Guid.NewGuid().ToString("N");
        var streamB = "no-tenant:Product:" + Guid.NewGuid().ToString("N");
        await ordering.AppendAsync(streamA, [ContractEvent.Tick(), ContractEvent.Tick(), ContractEvent.Tick()]);
        await catalog.AppendAsync(streamB, [ContractEvent.Tick(), ContractEvent.Tick()]);
        var orderEvents = await ordering.ReadStreamAsync(streamA);
        var catalogEvents = await catalog.ReadStreamAsync(streamB);
        Assert.Equal(3, orderEvents.Count);
        Assert.Equal(2, catalogEvents.Count);
        Assert.Equal(orderEvents[0].SequencePosition + 1, orderEvents[1].SequencePosition);
        Assert.Equal(orderEvents[0].SequencePosition + 2, orderEvents[2].SequencePosition);
        Assert.Equal(catalogEvents[0].SequencePosition + 1, catalogEvents[1].SequencePosition);
    }

    [Fact]
    public async Task ExpectedVersion_NegativeOne_CreatesAStream()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = "acme:Order:" + Guid.NewGuid().ToString("N");
        var result = await store.AppendAsync(
            streamId,
            [ContractEvent.Tick()],
            expectedVersion: -1,
            metadata: new EventMetadata { TenantId = "acme" });
        Assert.Equal(1, result.LastStreamVersion);
        var metadata = await store.GetStreamAsync(streamId);
        Assert.NotNull(metadata);
        Assert.Equal(1, metadata!.CurrentVersion);
    }

    [Fact]
    public async Task ExpectedVersion_NegativeOne_OnExistingStream_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = "acme:Order:" + Guid.NewGuid().ToString("N");
        var metadata = new EventMetadata { TenantId = "acme" };
        await store.AppendAsync(streamId, [ContractEvent.Tick()], expectedVersion: -1, metadata: metadata);
        var ex = await Assert.ThrowsAsync<ConcurrencyException>(() =>
            store.AppendAsync(streamId, [ContractEvent.Tick()], expectedVersion: -1, metadata: metadata));
        Assert.Equal(-1, ex.ExpectedPosition);
        Assert.Equal(1, ex.ActualPosition);
    }

    [Fact]
    public async Task ExpectedVersion_Zero_OnNewStream_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = "acme:Order:" + Guid.NewGuid().ToString("N");
        var ex = await Assert.ThrowsAsync<ConcurrencyException>(() =>
            store.AppendAsync(
                streamId,
                [ContractEvent.Tick()],
                expectedVersion: 0,
                metadata: new EventMetadata { TenantId = "acme" }));
        Assert.Equal(0, ex.ExpectedPosition);
        Assert.Equal(-1, ex.ActualPosition);
    }

    private static string Stream() => "test-tenant:TestAggregate:" + Guid.NewGuid().ToString("N");

    private sealed class ReloadGate
    {
        public volatile TaskCompletionSource? Current;
    }

    private static Task InitializeSchemaAsync(ContractEventStore opened)
    {
        if (opened.Store is SqlServerEventStore sql)
            return sql.InitializeSchemaAsync();
        return ((PostgresEventStore)opened.Store).InitializeSchemaAsync();
    }

    private static async Task<int> CountEventNamesAsync(ContractEventStore opened, string name)
    {
        if (opened.Store is SqlServerEventStore sql)
        {
            var hooks = (SqlServerContractHooks)opened.Hooks;
            await using var connection = new SqlConnection(hooks.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand(
                $"SELECT COUNT(*) FROM {sql.QualifiedTypesTable} WHERE Token = @Token",
                connection);
            command.Parameters.AddWithValue("@Token", name);
            return (int)(await command.ExecuteScalarAsync())!;
        }

        var postgres = (PostgresEventStore)opened.Store;
        await using var pg = new NpgsqlConnection(postgres.ConnectionString);
        await pg.OpenAsync();
        await using var query = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {postgres.QualifiedTypesTable} WHERE token = @token",
            pg);
        query.Parameters.AddWithValue("token", name);
        return Convert.ToInt32(await query.ExecuteScalarAsync());
    }
}
