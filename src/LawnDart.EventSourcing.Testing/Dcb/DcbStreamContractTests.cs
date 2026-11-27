using LawnDart;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Microsoft.Extensions.Options;

namespace LawnDart.EventSourcing.Testing;

/// <summary>
/// The fixed dynamic consistency boundary stream: <c>dcb</c>, or <c>{tenant}:dcb</c> with a tenant.
/// Every dynamic consistency boundary append writes to it, its versions only increase,
/// a direct stream read of it is refused, and it adds no stream registry entry.
/// </summary>
public abstract class DcbStreamContractTests : ContractSuite
{
    protected DcbStreamContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task ConditionalAppend_Shared_WritesToTenantDcbStream()
    {
        var store = await OpenSharedAsync();
        var tag = Tag("order");

        var append = await store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [tag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
            metadata: new EventMetadata { TenantId = "acme" });

        var evt = await ReadOneAsync(store, Query.All().ForTenant("acme"), append.SequencePositions[0]);
        Assert.Equal(StreamId.Dcb("acme"), evt.StreamId);
        Assert.True(evt.StreamId.IsDcb);
        Assert.Equal("acme", evt.StreamId.TenantId);
    }

    [Fact]
    public async Task ConditionalAppend_SingleWithoutTenant_WritesToPlainDcbStream()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Single))).Store;
        var tag = Tag("order");

        var append = await store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [tag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))));

        var evt = await ReadOneAsync(store, Query.All(), append.SequencePositions[0]);
        Assert.Equal(StreamId.Dcb(), evt.StreamId);
        Assert.Equal("dcb", evt.StreamId.ToString());
    }

    [Fact]
    public async Task ConditionalAppend_SingleWithTenant_WritesToThatTenantsDcbStream()
    {
        var store = (await Backend.OpenEventStoreAsync(
            ContractStores.Isolated(tenancy: Tenancy.Single, singleTenantId: "solo"))).Store;
        var tag = Tag("order");

        var append = await store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [tag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))));

        var evt = await ReadOneAsync(store, Query.All(), append.SequencePositions[0]);
        Assert.Equal(StreamId.Dcb("solo"), evt.StreamId);
    }

    [Fact]
    public async Task DcbStreamVersions_OnlyIncrease()
    {
        var store = await OpenSharedAsync();
        var metadata = new EventMetadata { TenantId = "acme" };

        await store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [Tag("order")]), new TaggedEvent(ContractEvent.Tick(), [Tag("order")])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(Tag("absent")))),
            metadata: metadata);
        await store.AppendAsync(StreamId.For("acme", "Order", Guid.NewGuid().ToString("N")), [ContractEvent.Tick()]);
        await store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [Tag("order")])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(Tag("absent")))),
            metadata: metadata);
        await store.AppendAsync(StreamId.Dcb("acme"), [new TaggedEvent(ContractEvent.Tick(), [Tag("order")])], metadata: metadata);

        var events = (await store.ReadByQueryAsync(Query.All().ForTenant("acme"))).Events
            .Where(e => e.StreamId.IsDcb)
            .OrderBy(e => e.SequencePosition)
            .ToList();

        Assert.Equal(4, events.Count);
        Assert.All(events, e => Assert.Equal(StreamId.Dcb("acme"), e.StreamId));
        for (var i = 1; i < events.Count; i++)
            Assert.True(events[i].Version > events[i - 1].Version, $"Version {events[i].Version} does not follow {events[i - 1].Version}.");
    }

    [Fact]
    public void Subscription_ToDcbStream_IsRefused()
    {
        // A stream subscription is a direct read, so the DCB stream is refused for every backend.
        foreach (var streamId in new[] { StreamId.Dcb("acme"), StreamId.Dcb() })
        {
            var refused = Assert.Throws<ArgumentException>(() => EventSubscriptionFilter.ForStream(streamId));
            Assert.Equal("streamId", refused.ParamName);
            Assert.Contains("dynamic consistency boundary stream", refused.Message);
        }
    }

    [ContractFact]
    public async Task QuerySubscription_SeesDcbStreamEvents()
    {
        SkipUnless(Cap.RunsLiveSubscriptionTests, "This backend does not run the live subscription suite yet.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared));
        var tag = Tag("order");
        var append = await opened.Store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [tag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
            metadata: new EventMetadata { TenantId = "acme" });

        await using var handle = SubscriptionsOf(opened).Subscribe(
            "dcb-query",
            fromSequence: 1,
            EventSubscriptionFilter.ForQuery(Query.FromItems(QueryItem.ByTags(tag)).ForTenant("acme")));
        var received = await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(15));
        Assert.Equal(append.SequencePositions[0], received[0].SequencePosition);
        Assert.Equal(StreamId.Dcb("acme"), received[0].StreamId);
    }

    [Fact]
    public async Task ReadStream_DcbStream_IsRefused()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared));
        var tag = Tag("order");
        await opened.Store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [tag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
            metadata: new EventMetadata { TenantId = "acme" });

        foreach (var streamId in new[] { StreamId.Dcb("acme"), StreamId.Dcb() })
        {
            var typed = await Assert.ThrowsAsync<ArgumentException>(() => opened.Store.ReadStreamAsync(streamId));
            Assert.Contains("dynamic consistency boundary stream", typed.Message);
            await Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                await foreach (var _ in opened.Store.ReadStreamEnumerableAsync(streamId))
                {
                }
            });
            await Assert.ThrowsAsync<ArgumentException>(() => opened.Log.ReadStreamAsync(streamId));
            await Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                await foreach (var _ in opened.Log.ReadStreamEnumerableAsync(streamId))
                {
                }
            });
        }
    }

    [Fact]
    public async Task AppendToDcbStream_IsUnconditionalDcbAppend()
    {
        var store = await OpenSharedAsync();
        var tag = Tag("order");

        var append = await store.AppendAsync(
            StreamId.Dcb("acme"),
            [new TaggedEvent(ContractEvent.Tick(), [tag])],
            metadata: new EventMetadata { TenantId = "acme" });

        var evt = await ReadOneAsync(store, Query.All().ForTenant("acme"), append.SequencePositions[0]);
        Assert.Equal(StreamId.Dcb("acme"), evt.StreamId);

        // A later condition on the same tag sees the event.
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [tag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
            metadata: new EventMetadata { TenantId = "acme" }));
    }

    [Fact]
    public async Task AppendToDcbStream_WithExpectedVersion_ThrowsAndWritesNothing()
    {
        var store = await OpenSharedAsync();

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(
            StreamId.Dcb("acme"),
            [new TaggedEvent(ContractEvent.Tick(), [Tag("order")])],
            expectedVersion: 0,
            metadata: new EventMetadata { TenantId = "acme" }));

        Assert.Equal("expectedVersion", ex.ParamName);
        Assert.Equal(0, await store.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task AppendToDcbStream_SharedWithoutTenant_Throws()
    {
        var store = await OpenSharedAsync();

        await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(
            StreamId.Dcb(),
            [new TaggedEvent(ContractEvent.Tick(), [Tag("order")])]));

        Assert.Equal(0, await store.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task AppendToDcbStream_TenantMismatch_Throws()
    {
        var store = await OpenSharedAsync();

        await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(
            StreamId.Dcb("acme"),
            [new TaggedEvent(ContractEvent.Tick(), [Tag("order")])],
            metadata: new EventMetadata { TenantId = "globex" }));

        Assert.Equal(0, await store.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task DcbAppends_AddNoStreamRegistryEntries()
    {
        var store = await OpenSharedAsync();
        var aggregate = StreamId.For("acme", "Order", Guid.NewGuid().ToString("N"));
        await store.AppendAsync(aggregate, [ContractEvent.Tick()]);
        var before = await store.GetStreamCountAsync();

        for (var i = 0; i < 5; i++)
        {
            var tag = Tag("order");
            await store.AppendAsync(
                [new TaggedEvent(ContractEvent.Tick(), [tag])],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
                metadata: new EventMetadata { TenantId = "acme" });
        }

        var unconditionalTag = Tag("order");
        await store.AppendAsync(
            StreamId.Dcb("acme"),
            [new TaggedEvent(ContractEvent.Tick(), [unconditionalTag])],
            metadata: new EventMetadata { TenantId = "acme" });

        Assert.Equal(before, await store.GetStreamCountAsync());
        Assert.Empty(await store.GetStreamsByTagAsync(unconditionalTag));
        Assert.Null(await store.GetStreamAsync(StreamId.Dcb("acme")));
        var ids = await store.EnumerateStreamIdsAsync();
        Assert.Equal([aggregate], ids);
    }

    [Fact]
    public async Task RetriedAppend_IsCaughtByTheCondition_NotByEventId()
    {
        var store = await OpenSharedAsync();
        var tag = Tag("order");
        var head = await store.GetCurrentSequenceAsync();
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag)).ForTenant("acme"), after: head);

        await store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [tag])],
            condition,
            metadata: new EventMetadata { TenantId = "acme" });

        // The retry builds new events, so new event ids. The condition still sees the first write.
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [tag])],
            condition,
            metadata: new EventMetadata { TenantId = "acme" }));

        var matched = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tag)).ForTenant("acme"));
        Assert.Single(matched.Events);
    }

    [Fact]
    public async Task DcbRepository_UnconditionalAppend_UsesTenantDcbStream()
    {
        var store = await OpenSharedAsync();
        var repository = Repository(store, "acme");

        var positions = await repository.AppendWithContextAsync(
            [new TaggedEvent(ContractEvent.Tick(), [Tag("order")])],
            condition: null,
            new CommandMetadata { TenantId = "acme" });

        var evt = await ReadOneAsync(store, Query.All().ForTenant("acme"), positions[0]);
        Assert.Equal(StreamId.Dcb("acme"), evt.StreamId);
    }

    [Fact]
    public async Task DcbRepository_ConditionTenantOnly_UsesThatTenantsDcbStream()
    {
        var store = await OpenSharedAsync();
        var repository = Repository(store, tenantId: null);
        var tag = Tag("order");

        var positions = await repository.AppendWithContextAsync(
            [new TaggedEvent(ContractEvent.Tick(), [tag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag)).ForTenant("acme")),
            new CommandMetadata());

        var evt = await ReadOneAsync(store, Query.All().ForTenant("acme"), positions[0]);
        Assert.Equal(StreamId.Dcb("acme"), evt.StreamId);
    }

    private async Task<IEventStore> OpenSharedAsync()
        => (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;

    private static async Task<SequencedEvent> ReadOneAsync(IEventStore store, Query query, long position)
    {
        var read = await store.ReadByQueryAsync(query, fromSequencePosition: position, limit: 1);
        return Assert.Single(read.Events);
    }

    private static DcbRepository Repository(IEventStore store, string? tenantId)
        => new(
            store,
            new ContractMetadataProvider(new ContractTenantContextProvider(tenantId)),
            new ContractTenantContextProvider(tenantId),
            Options.Create(new LawnDartOptions { Tenancy = Tenancy.Shared }));

    private static LawnDart.EventStore.Tag Tag(string kind) => LawnDart.EventStore.Tag.Create(kind, Guid.NewGuid().ToString("N"));
}
