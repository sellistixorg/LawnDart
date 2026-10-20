using LawnDart.EventStore;
using LawnDart.Metadata;
using Microsoft.Data.SqlClient;

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
            "In-memory appends run one at a time, so a parallel append does not hit a duplicate version.");

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
        Assert.Equal([1L, 2L, 3L], versions);
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
        var both = "both:" + Guid.NewGuid().ToString("N");
        var only = "only:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [both, only]);
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [both]);
        var result = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(both, only)));
        Assert.Single(result.Events);
        Assert.Contains(both, result.Events[0].Tags);
        Assert.Contains(only, result.Events[0].Tags);
    }

    [Fact]
    public async Task ReadByQueryAsync_WithOrTagItems_ReturnsUnion()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var left = "left:" + Guid.NewGuid().ToString("N");
        var right = "right:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [left]);
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [right]);
        var result = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(left), QueryItem.ByTags(right)));
        Assert.Equal(2, result.Events.Select(e => e.SequencePosition).Distinct().Count());
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
        var first = await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [tag]);
        var second = await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [tag]);
        Assert.Equal(second.SequencePositions[0], await store.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags(tag))));
        Assert.Equal(
            second.SequencePositions[0],
            await store.GetMaxSequencePositionAsync(
                Query.FromItems(QueryItem.ByTags(tag)),
                fromSequencePosition: first.SequencePositions[0] + 1));
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
        await opened.Log.AppendAsync(streamId, [new AppendEvent(token, "{}"u8.ToArray())]);
        var recorded = Assert.Single(await opened.Log.ReadStreamAsync(streamId));
        Assert.Equal(token, recorded.EventType);
        await Assert.ThrowsAsync<UnknownEventFamilyException>(() => opened.Store.ReadStreamAsync(streamId));
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

    [ContractFact]
    public async Task AppendAsync_ReservedTenantDefault_Throws()
    {
        SkipUnless(Cap.ChecksTenantIdOnAppend, "In-memory append does not check tenant ids.");
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(requireTenantId: true))).Store;
        var streamId = $"default:Order:{Guid.NewGuid()}";
        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync(streamId, [ContractEvent.Tick()], metadata: new EventMetadata { TenantId = "default" }));
        Assert.Contains("reserved", ex.Message);
    }

    [ContractFact]
    public async Task AppendAsync_ValidateNamesFalse_AllowsReservedTenantDefault()
    {
        SkipUnless(Cap.ChecksTenantIdOnAppend, "In-memory append does not check tenant ids.");
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(requireTenantId: true, validateNames: false))).Store;
        var streamId = $"default:Order:{Guid.NewGuid()}";
        await store.AppendAsync(streamId, [ContractEvent.Tick()], metadata: new EventMetadata { TenantId = "default" });
        Assert.Single(await store.ReadStreamAsync(streamId));
        var stream = await store.GetStreamAsync(streamId);
        Assert.Equal("default", stream!.TenantId);
    }

    [ContractFact]
    public async Task AppendAsync_EmptyTenantId_Throws()
    {
        SkipUnless(Cap.ChecksTenantIdOnAppend, "In-memory append does not check tenant ids.");
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(requireTenantId: true, validateNames: false))).Store;
        var streamId = $":Order:{Guid.NewGuid()}";
        await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(streamId, [ContractEvent.Tick()]));
    }

    [ContractFact]
    public async Task AppendAsync_MissingTenant_WhenRequired_Throws()
    {
        SkipUnless(Cap.ChecksTenantIdOnAppend, "In-memory append does not check tenant ids.");
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

    private static string Stream() => "test-tenant:TestAggregate:" + Guid.NewGuid().ToString("N");
}
