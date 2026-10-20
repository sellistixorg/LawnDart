using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Backends.Contract.Tests;

public abstract class StreamRegistryContractTests : ContractSuite
{
    protected StreamRegistryContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task GetStreamAsync_AfterAppend_ReturnsMetadata()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = "test-tenant:Order:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(streamId, [ContractEvent.Tick()], metadata: new EventMetadata { TenantId = "test-tenant" });
        var metadata = await store.GetStreamAsync(streamId);
        Assert.NotNull(metadata);
        Assert.Equal(streamId, metadata!.StreamId);
        Assert.Equal("test-tenant", metadata.TenantId);
        Assert.Equal("Order", metadata.AggregateType);
        Assert.Equal(1, metadata.CurrentVersion);
        Assert.Equal(1, metadata.EventCount);
    }

    [Fact]
    public async Task GetStreamsByAggregateTypeAsync_ReturnsMatchingStreams()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var metadata = new EventMetadata { TenantId = "test-tenant" };
        var first = "test-tenant:Order:" + Guid.NewGuid().ToString("N");
        var second = "test-tenant:Order:" + Guid.NewGuid().ToString("N");
        var other = "test-tenant:Customer:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(first, [ContractEvent.Tick()], metadata: metadata);
        await store.AppendAsync(second, [ContractEvent.Tick()], metadata: metadata);
        await store.AppendAsync(other, [ContractEvent.Tick()], metadata: metadata);
        var orders = await store.GetStreamsByAggregateTypeAsync("Order");
        Assert.Equal(2, orders.Count);
        Assert.All(orders, s => Assert.Equal("Order", s.AggregateType));
        Assert.All(orders, s => Assert.Equal("test-tenant", s.TenantId));
    }

    [Fact]
    public async Task GetStreamsByTagAsync_ReturnsMatchingStreams()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var metadata = new EventMetadata { TenantId = "test-tenant" };
        var high = "high-" + Guid.NewGuid().ToString("N");
        var low = "low-" + Guid.NewGuid().ToString("N");
        var match = "test-tenant:Order:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(match, [ContractEvent.Tick()], metadata: metadata, tags: [high]);
        await store.AppendAsync("test-tenant:Order:" + Guid.NewGuid().ToString("N"), [ContractEvent.Tick()], metadata: metadata, tags: [low]);
        var found = await store.GetStreamsByTagAsync(high);
        Assert.Single(found);
        Assert.Contains(high, found[0].Tags);
        Assert.Equal("test-tenant", found[0].TenantId);
    }

    [Fact]
    public async Task EnumerateStreamIdsAsync_ReturnsAllStreamIds()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var metadata = new EventMetadata { TenantId = "test-tenant" };
        var first = "test-tenant:Order:" + Guid.NewGuid().ToString("N");
        var second = "test-tenant:Order:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(first, [ContractEvent.Tick()], metadata: metadata);
        await store.AppendAsync(second, [ContractEvent.Tick()], metadata: metadata);
        var ids = await store.EnumerateStreamIdsAsync();
        Assert.Equal(2, ids.Count);
        Assert.Contains(first, ids);
        Assert.Contains(second, ids);
    }

    [Fact]
    public async Task PrefixQueries_TreatLikeMetacharactersAsLiteral()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var suffix = Guid.NewGuid().ToString("N");
        await store.AppendAsync($"acme_west{suffix}:Order:1", [ContractEvent.Tick()]);
        await store.AppendAsync($"acmeXwest{suffix}:Order:1", [ContractEvent.Tick()]);
        await store.AppendAsync($"acme%west{suffix}:Order:1", [ContractEvent.Tick()]);
        await store.AppendAsync($"acme[west{suffix}:Order:1", [ContractEvent.Tick()]);
        await store.AppendAsync($"ACME_WEST{suffix}:Order:2", [ContractEvent.Tick()]);

        var underscore = await store.EnumerateStreamIdsAsync($"acme_west{suffix}:");
        Assert.Equal(2, underscore.Count);
        Assert.Contains($"acme_west{suffix}:Order:1", underscore);
        Assert.DoesNotContain($"acmeXwest{suffix}:Order:1", underscore);
        Assert.Contains($"ACME_WEST{suffix}:Order:2", underscore);
        Assert.Equal(2L, await store.GetStreamCountAsync($"acme_west{suffix}:"));
        Assert.Equal(1L, await store.GetStreamCountAsync($"acmeXwest{suffix}:"));
        Assert.Equal(1L, await store.GetStreamCountAsync($"acme%west{suffix}:"));
        Assert.Equal(1L, await store.GetStreamCountAsync($"acme[west{suffix}:"));
        Assert.Equal(2L, await store.GetStreamCountAsync("acme_"));

        Assert.Equal([$"acme%west{suffix}:Order:1"], await store.EnumerateStreamIdsAsync($"acme%west{suffix}:"));
        Assert.Equal([$"acme[west{suffix}:Order:1"], await store.EnumerateStreamIdsAsync($"acme[west{suffix}:"));
    }
}
