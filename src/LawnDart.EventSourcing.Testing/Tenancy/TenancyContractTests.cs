using LawnDart;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.EventSourcing.Testing;

/// <summary>
/// Cross-backend contract tests for <see cref="Tenancy"/> modes,
/// <see cref="Query.TenantId"/>, and <see cref="Query.ForAllTenants()"/>.
/// </summary>
public abstract class TenancyContractTests : ContractSuite
{
    protected TenancyContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task Shared_QueryWithoutTenantOrAllTenants_IsRejectedOnAllEntryPoints()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared));
        var store = opened.Store;
        var log = opened.Log;
        var tagQuery = Query.FromItems(QueryItem.ByTags(Tag.Parse("order:1")));

        await Assert.ThrowsAsync<ArgumentException>(() => store.ReadByQueryAsync(Query.All()));
        await Assert.ThrowsAsync<ArgumentException>(() => store.ReadByQueryAsync(tagQuery));
        await Assert.ThrowsAsync<ArgumentException>(() => log.ReadByQueryAsync(Query.All()));
        await Assert.ThrowsAsync<ArgumentException>(() => store.GetMaxSequencePositionAsync(Query.All()));
        await Assert.ThrowsAsync<ArgumentException>(() => log.GetMaxSequencePositionAsync(tagQuery));
        Assert.Throws<ArgumentException>(() => store.ReadByQueryStreamAsync(Query.All()));
        Assert.Throws<ArgumentException>(() => log.ReadByQueryStreamAsync(tagQuery));

        if (store is IEventLogSubscriptions subs)
        {
            Assert.Throws<ArgumentException>(() =>
                subs.Subscribe("tenancy-unscoped", 1, EventSubscriptionFilter.ForQuery(Query.All())));
            Assert.Throws<ArgumentException>(() =>
                subs.Subscribe("tenancy-unscoped-tag", 1, EventSubscriptionFilter.ForQuery(tagQuery)));
        }
    }

    [Fact]
    public async Task Shared_QueryWithBothTenantIdAndAllTenants_IsRejected()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared));
        var store = opened.Store;
        Assert.Throws<InvalidOperationException>(() => Query.ForAllTenants().ForTenant("t1"));
        var invalid = System.Text.Json.JsonSerializer.Deserialize<Query>("""{"TenantId":"t1","AllTenants":true}""")!;

        await Assert.ThrowsAsync<ArgumentException>(() => store.ReadByQueryAsync(invalid));
        await Assert.ThrowsAsync<ArgumentException>(() => store.GetMaxSequencePositionAsync(invalid));
        Assert.Throws<ArgumentException>(() => store.ReadByQueryStreamAsync(invalid));

        if (store is IEventLogSubscriptions subs)
        {
            Assert.Throws<ArgumentException>(() =>
                subs.Subscribe("tenancy-both", 1, EventSubscriptionFilter.ForQuery(invalid)));
        }

        await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(invalid),
            metadata: new EventMetadata { TenantId = "t1" }));
    }

    [Fact]
    public async Task Shared_ForTenant_IsolatesReadsMaxPositionAndSubscriptions()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared));
        var store = opened.Store;
        var sharedTag = Tag.Create("order", Guid.NewGuid().ToString("N"));
        var typeName = EventTypeCatalog.TryGetDeclaredName(typeof(ContractTestEvent))!;

        var t1First = await store.AppendAsync(
            StreamId.For("t1", "Order", "1"),
            [new TaggedEvent(ContractEvent.Tick(), [sharedTag])]);
        var t2First = await store.AppendAsync(
            StreamId.For("t2", "Order", "1"),
            [new TaggedEvent(ContractEvent.Tick(), [sharedTag])]);
        var t1Second = await store.AppendAsync(
            StreamId.For("t1", "Order", "2"),
            [new TaggedEvent(new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow), [sharedTag])]);

        // Query.All().ForTenant("t1") (0 items) returns only t1 events.
        var allT1 = await store.ReadByQueryAsync(Query.All().ForTenant("t1"));
        Assert.Equal([t1First.SequencePositions[0], t1Second.SequencePositions[0]], allT1.Events.Select(e => e.SequencePosition).ToArray());
        Assert.All(allT1.Events, e => Assert.Equal("t1", e.StreamId.TenantId));

        // Tag query scoped to t2 returns only t2.
        var tagT2 = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(sharedTag)).ForTenant("t2"));
        Assert.Equal([t2First.SequencePositions[0]], tagT2.Events.Select(e => e.SequencePosition).ToArray());

        // Type query scoped to t1 returns only t1.
        var typeT1 = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByType(typeName)).ForTenant("t1"));
        Assert.Equal([t1First.SequencePositions[0]], typeT1.Events.Select(e => e.SequencePosition).ToArray());

        // Streaming read matches buffered read.
        var streamedT1 = new List<long>();
        await foreach (var evt in store.ReadByQueryStreamAsync(Query.FromItems(QueryItem.ByTags(sharedTag)).ForTenant("t1")))
            streamedT1.Add(evt.SequencePosition);
        Assert.Equal([t1First.SequencePositions[0], t1Second.SequencePositions[0]], streamedT1.ToArray());

        // Max sequence position respects tenant scope.
        Assert.Equal(t2First.SequencePositions[0], await store.GetMaxSequencePositionAsync(Query.All().ForTenant("t2")));
        Assert.Equal(t2First.SequencePositions[0], await store.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags(sharedTag)).ForTenant("t2")));
        Assert.Equal(0L, await store.GetMaxSequencePositionAsync(Query.All().ForTenant("t3")));

        // Query subscription scoped to t1 skips t2.
        if (store is IEventLogSubscriptions subs)
        {
            await using var sub = subs.Subscribe(
                "tenancy-sub-t1-" + Guid.NewGuid().ToString("N"),
                1,
                EventSubscriptionFilter.ForQuery(Query.FromItems(QueryItem.ByTags(sharedTag)).ForTenant("t1")));
            var delivered = await ReadRecordedAsync(sub, 2, TimeSpan.FromSeconds(10));
            Assert.Equal([t1First.SequencePositions[0], t1Second.SequencePositions[0]], delivered.Select(e => e.SequencePosition).ToArray());
            var extra = await DrainRecordedAsync(sub, TimeSpan.FromMilliseconds(250));
            Assert.Empty(extra);
        }
    }

    [Fact]
    public async Task Shared_ForAllTenants_ReadsAndSubscribesAcrossTenants()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared));
        var store = opened.Store;
        var sharedTag = Tag.Create("item", Guid.NewGuid().ToString("N"));
        var typeName = EventTypeCatalog.TryGetDeclaredName(typeof(ContractTestEvent))!;

        var t1 = await store.AppendAsync(StreamId.For("t1", "Order", "1"), [new TaggedEvent(ContractEvent.Tick(), [sharedTag])]);
        var t2 = await store.AppendAsync(StreamId.For("t2", "Order", "1"), [new TaggedEvent(ContractEvent.Tick(), [sharedTag])]);

        var all = await store.ReadByQueryAsync(Query.ForAllTenants());
        Assert.Equal([t1.SequencePositions[0], t2.SequencePositions[0]], all.Events.Select(e => e.SequencePosition).ToArray());

        var byTag = await store.ReadByQueryAsync(Query.ForAllTenants(QueryItem.ByTags(sharedTag)));
        Assert.Equal([t1.SequencePositions[0], t2.SequencePositions[0]], byTag.Events.Select(e => e.SequencePosition).ToArray());

        var byType = await store.ReadByQueryAsync(Query.ForAllTenants(QueryItem.ByType(typeName)));
        Assert.Equal([t1.SequencePositions[0], t2.SequencePositions[0]], byType.Events.Select(e => e.SequencePosition).ToArray());

        var streamed = new List<long>();
        await foreach (var evt in store.ReadByQueryStreamAsync(Query.ForAllTenants(QueryItem.ByTags(sharedTag))))
            streamed.Add(evt.SequencePosition);
        Assert.Equal([t1.SequencePositions[0], t2.SequencePositions[0]], streamed.ToArray());

        Assert.Equal(t2.SequencePositions[0], await store.GetMaxSequencePositionAsync(Query.ForAllTenants()));
        Assert.Equal(t2.SequencePositions[0], await store.GetMaxSequencePositionAsync(Query.ForAllTenants(QueryItem.ByTags(sharedTag))));

        if (store is IEventLogSubscriptions subs)
        {
            await using var sub = subs.Subscribe(
                "tenancy-sub-all-" + Guid.NewGuid().ToString("N"),
                1,
                EventSubscriptionFilter.ForQuery(Query.ForAllTenants(QueryItem.ByTags(sharedTag))));
            var delivered = await ReadRecordedAsync(sub, 2, TimeSpan.FromSeconds(10));
            Assert.Equal([t1.SequencePositions[0], t2.SequencePositions[0]], delivered.Select(e => e.SequencePosition).ToArray());
        }
    }

    [Fact]
    public async Task Shared_DcbAppendCondition_ScopesToAppendTenant_RejectsCrossTenant_AllowsForAllTenants()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared));
        var store = opened.Store;
        var itemTag = Tag.Create("item", Guid.NewGuid().ToString("N"));
        var typeName = EventTypeCatalog.TryGetDeclaredName(typeof(ContractTestEvent))!;

        await store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [itemTag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(itemTag))),
            metadata: new EventMetadata { TenantId = "t1" });

        // Same tag on t2 succeeds because condition defaults to append tenant t2.
        var t2Result = await store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [itemTag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(itemTag))),
            metadata: new EventMetadata { TenantId = "t2" });
        Assert.Single(t2Result.SequencePositions);

        // Same tag on t1 conflicts against t1's earlier event.
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [itemTag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(itemTag))),
            metadata: new EventMetadata { TenantId = "t1" }));

        // Type-only condition is also tenant-scoped.
        var t3TypeResult = await store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [itemTag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType(typeName))),
            metadata: new EventMetadata { TenantId = "t3" });
        Assert.Single(t3TypeResult.SequencePositions);

        // Condition pointing at a different tenant than metadata.TenantId is rejected.
        await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [itemTag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(itemTag)).ForTenant("t1")),
            metadata: new EventMetadata { TenantId = "t2" }));

        // Condition built with Query.ForAllTenants(...) sees across all tenants.
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [itemTag])],
            AppendCondition.FailIfMatches(Query.ForAllTenants(QueryItem.ByTags(itemTag))),
            metadata: new EventMetadata { TenantId = "t4" }));

        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [itemTag])],
            AppendCondition.FailIfMatches(Query.ForAllTenants(QueryItem.ByType(typeName))),
            metadata: new EventMetadata { TenantId = "t4" }));
    }

    [Fact]
    public async Task Single_WithoutSingleTenantId_RejectsTenantsAndReadsWholeStore()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Single));
        var store = opened.Store;
        var tag = Tag.Create("item", Guid.NewGuid().ToString("N"));

        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync(StreamId.For("t1", "Order", "1"), [ContractEvent.Tick()]));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync(StreamId.For("Order", "1"), [ContractEvent.Tick()], metadata: new EventMetadata { TenantId = "t1" }));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.ReadByQueryAsync(Query.All().ForTenant("t1")));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.GetMaxSequencePositionAsync(Query.All().ForTenant("t1")));
        Assert.Throws<ArgumentException>(() =>
            store.ReadByQueryStreamAsync(Query.All().ForTenant("t1")));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync(
                [new TaggedEvent(ContractEvent.Tick(), [tag])],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
                metadata: new EventMetadata { TenantId = "t1" }));

        var streamId = StreamId.For("Order", "1");
        var first = await store.AppendAsync(streamId, [new TaggedEvent(ContractEvent.Tick(), [tag])]);
        var dcb = await store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [Tag.Parse("kind:dcb")])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(Tag.Parse("kind:dcb")))));

        var streamMeta = await store.GetStreamAsync(streamId);
        Assert.NotNull(streamMeta);
        Assert.Null(streamMeta!.TenantId);

        Assert.Equal(2, (await store.ReadByQueryAsync(Query.All())).Events.Count);
        Assert.Equal(2, (await store.ReadByQueryAsync(Query.ForAllTenants())).Events.Count);
        Assert.Equal([first.SequencePositions[0]], (await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tag)))).Events.Select(e => e.SequencePosition).ToArray());
        Assert.Equal(dcb.SequencePositions[0], await store.GetMaxSequencePositionAsync(Query.All()));
    }

    [Fact]
    public async Task Single_WithSingleTenantId_AllowsTwoPartAndMatchingThreePart_RejectsForeignTenant()
    {
        var opened = await Backend.OpenEventStoreAsync(
            ContractStores.Isolated(tenancy: Tenancy.Single, singleTenantId: "acme"));
        var store = opened.Store;
        var sharedTag = Tag.Create("order", Guid.NewGuid().ToString("N"));
        var typeName = EventTypeCatalog.TryGetDeclaredName(typeof(ContractTestEvent))!;

        // Reject foreign tenant on append, metadata, query, and DCB.
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync(StreamId.For("other", "Order", "1"), [ContractEvent.Tick()]));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync(StreamId.For("Order", "1"), [ContractEvent.Tick()], metadata: new EventMetadata { TenantId = "other" }));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.ReadByQueryAsync(Query.All().ForTenant("other")));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.GetMaxSequencePositionAsync(Query.All().ForTenant("other")));
        Assert.Throws<ArgumentException>(() =>
            store.ReadByQueryStreamAsync(Query.All().ForTenant("other")));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync(
                [new TaggedEvent(ContractEvent.Tick(), [sharedTag])],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(sharedTag))),
                metadata: new EventMetadata { TenantId = "other" }));

        var twoPart = StreamId.For("Order", "1");
        var threePart = StreamId.For("acme", "Order", "2");
        var p1 = await store.AppendAsync(twoPart, [new TaggedEvent(ContractEvent.Tick(), [sharedTag])]);
        var p2 = await store.AppendAsync(
            threePart,
            [new TaggedEvent(ContractEvent.Tick(), [sharedTag])],
            metadata: new EventMetadata { TenantId = "acme" });

        // Stream metadata TenantId reflects each stream's own id.
        Assert.Null((await store.GetStreamAsync(twoPart))!.TenantId);
        Assert.Equal("acme", (await store.GetStreamAsync(threePart))!.TenantId);

        // Queries without tenant, with ForTenant("acme"), and with ForAllTenants() all see both streams.
        var expected = new[] { p1.SequencePositions[0], p2.SequencePositions[0] };
        Assert.Equal(expected, (await store.ReadByQueryAsync(Query.All())).Events.Select(e => e.SequencePosition).ToArray());
        Assert.Equal(expected, (await store.ReadByQueryAsync(Query.All().ForTenant("acme"))).Events.Select(e => e.SequencePosition).ToArray());
        Assert.Equal(expected, (await store.ReadByQueryAsync(Query.ForAllTenants())).Events.Select(e => e.SequencePosition).ToArray());
        Assert.Equal(expected, (await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(sharedTag)).ForTenant("acme"))).Events.Select(e => e.SequencePosition).ToArray());
        Assert.Equal(expected, (await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByType(typeName)).ForTenant("acme"))).Events.Select(e => e.SequencePosition).ToArray());

        var streamed = new List<long>();
        await foreach (var evt in store.ReadByQueryStreamAsync(Query.FromItems(QueryItem.ByTags(sharedTag)).ForTenant("acme")))
            streamed.Add(evt.SequencePosition);
        Assert.Equal(expected, streamed.ToArray());

        Assert.Equal(p2.SequencePositions[0], await store.GetMaxSequencePositionAsync(Query.All().ForTenant("acme")));

        if (store is IEventLogSubscriptions subs)
        {
            await using var sub = subs.Subscribe(
                "tenancy-single-acme-" + Guid.NewGuid().ToString("N"),
                1,
                EventSubscriptionFilter.ForQuery(Query.FromItems(QueryItem.ByTags(sharedTag)).ForTenant("acme")));
            var delivered = await ReadRecordedAsync(sub, 2, TimeSpan.FromSeconds(10));
            Assert.Equal(expected, delivered.Select(e => e.SequencePosition).ToArray());
        }

        // DCB condition sees the 2-part and 3-part events regardless of whether TenantId is null or "acme".
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [sharedTag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(sharedTag)))));
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [sharedTag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(sharedTag)).ForTenant("acme")),
            metadata: new EventMetadata { TenantId = "acme" }));
    }

    [Fact]
    public async Task Isolated_IsRefusedAtStartup()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Isolated)));
    }
}
