using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.EventSourcing.Testing;

public abstract class StreamRegistryContractTests : ContractSuite
{
    protected StreamRegistryContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task GetStreamAsync_AfterAppend_ReturnsMetadata()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var streamId = "test-tenant:Order:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(StreamId.Parse(streamId), [ContractEvent.Tick()], metadata: new EventMetadata { TenantId = "test-tenant" });
        var metadata = await store.GetStreamAsync(StreamId.Parse(streamId));
        Assert.NotNull(metadata);
        Assert.Equal(streamId, metadata!.StreamId.ToString());
        Assert.Equal("test-tenant", metadata.TenantId);
        Assert.Equal("Order", metadata.AggregateType);
        Assert.Equal(1, metadata.CurrentVersion);
        Assert.Equal(1, metadata.EventCount);
    }

    [Fact]
    public async Task GetStreamsByAggregateTypeAsync_ReturnsMatchingStreams()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var metadata = new EventMetadata { TenantId = "test-tenant" };
        var first = "test-tenant:Order:" + Guid.NewGuid().ToString("N");
        var second = "test-tenant:Order:" + Guid.NewGuid().ToString("N");
        var other = "test-tenant:Customer:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(StreamId.Parse(first), [ContractEvent.Tick()], metadata: metadata);
        await store.AppendAsync(StreamId.Parse(second), [ContractEvent.Tick()], metadata: metadata);
        await store.AppendAsync(StreamId.Parse(other), [ContractEvent.Tick()], metadata: metadata);
        var orders = await store.GetStreamsByAggregateTypeAsync("Order");
        Assert.Equal(2, orders.Count);
        Assert.All(orders, s => Assert.Equal("Order", s.AggregateType));
        Assert.All(orders, s => Assert.Equal("test-tenant", s.TenantId));
    }

    [Fact]
    public async Task GetStreamsByTagAsync_ReturnsMatchingStreams()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var metadata = new EventMetadata { TenantId = "test-tenant" };
        var high = LawnDart.EventStore.Tag.Parse("high:" + Guid.NewGuid().ToString("N"));
        var low = LawnDart.EventStore.Tag.Parse("low:" + Guid.NewGuid().ToString("N"));
        var match = "test-tenant:Order:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(StreamId.Parse(match), [new TaggedEvent(ContractEvent.Tick(), [high])], metadata: metadata);
        await store.AppendAsync(StreamId.For("test-tenant", "Order", Guid.NewGuid().ToString("N")), [new TaggedEvent(ContractEvent.Tick(), [low])], metadata: metadata);
        var found = await store.GetStreamsByTagAsync(high);
        Assert.Single(found);
        Assert.Contains(high, found[0].Tags);
        Assert.Equal("test-tenant", found[0].TenantId);
    }

    [Fact]
    public async Task EnumerateStreamIdsAsync_ReturnsAllStreamIds()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var metadata = new EventMetadata { TenantId = "test-tenant" };
        var first = "test-tenant:Order:" + Guid.NewGuid().ToString("N");
        var second = "test-tenant:Order:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(StreamId.Parse(first), [ContractEvent.Tick()], metadata: metadata);
        await store.AppendAsync(StreamId.Parse(second), [ContractEvent.Tick()], metadata: metadata);
        var ids = await store.EnumerateStreamIdsAsync();
        Assert.Equal(2, ids.Count);
        Assert.Contains(first, ids.Select(static s => s.ToString()).ToList());
        Assert.Contains(second, ids.Select(static s => s.ToString()).ToList());
    }

    [Fact]
    public async Task PrefixQueries_TreatLikeMetacharactersAsLiteral()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared, validateNames: false))).Store;
        var suffix = Guid.NewGuid().ToString("N");
        await store.AppendAsync(StreamId.Parse($"acme_west{suffix}:Order:1"), [ContractEvent.Tick()]);
        await store.AppendAsync(StreamId.Parse($"acmeXwest{suffix}:Order:1"), [ContractEvent.Tick()]);
        await store.AppendAsync(StreamId.Parse($"acme%west{suffix}:Order:1"), [ContractEvent.Tick()]);
        await store.AppendAsync(StreamId.Parse($"acme[west{suffix}:Order:1"), [ContractEvent.Tick()]);
        await store.AppendAsync(StreamId.Parse($"ACME_WEST{suffix}:Order:2"), [ContractEvent.Tick()]);

        var underscore = await store.EnumerateStreamIdsAsync($"acme_west{suffix}:");
        Assert.Contains($"acme_west{suffix}:Order:1", underscore.Select(static s => s.ToString()).ToList());
        Assert.DoesNotContain($"acmeXwest{suffix}:Order:1", underscore.Select(static s => s.ToString()).ToList());
        if (Cap.StreamIdPrefixCaseInsensitive)
        {
            Assert.Equal(2, underscore.Count);
            Assert.Contains($"ACME_WEST{suffix}:Order:2", underscore.Select(static s => s.ToString()).ToList());
            Assert.Equal(2L, await store.GetStreamCountAsync($"acme_west{suffix}:"));
        }
        else
        {
            Assert.Single(underscore);
            Assert.DoesNotContain($"ACME_WEST{suffix}:Order:2", underscore.Select(static s => s.ToString()).ToList());
            Assert.Equal(1L, await store.GetStreamCountAsync($"acme_west{suffix}:"));
        }
        Assert.Equal(1L, await store.GetStreamCountAsync($"acmeXwest{suffix}:"));
        Assert.Equal(1L, await store.GetStreamCountAsync($"acme%west{suffix}:"));
        Assert.Equal(1L, await store.GetStreamCountAsync($"acme[west{suffix}:"));
        Assert.Equal(Cap.StreamIdPrefixCaseInsensitive ? 2L : 1L, await store.GetStreamCountAsync("acme_"));

        Assert.Equal([$"acme%west{suffix}:Order:1"], (await store.EnumerateStreamIdsAsync($"acme%west{suffix}:")).Select(static s => s.ToString()));
        Assert.Equal([$"acme[west{suffix}:Order:1"], (await store.EnumerateStreamIdsAsync($"acme[west{suffix}:")).Select(static s => s.ToString()));
    }
}
