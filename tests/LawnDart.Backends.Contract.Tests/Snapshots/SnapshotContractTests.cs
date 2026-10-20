using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventSourcing;
using LawnDart.EventStore;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.Snapshots;
using LawnDart.Metadata;
using LawnDart.Snapshots;
using Microsoft.Extensions.Options;

namespace LawnDart.Backends.Contract.Tests;

public abstract class SnapshotContractTests : ContractSuite
{
    protected SnapshotContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task SaveThenLoad_RoundTripsStreamState()
    {
        var opened = await Backend.OpenSnapshotsAsync(ContractStores.Isolated());
        var streamId = "acct:Order:" + Guid.NewGuid().ToString("N");
        var state = new InventoryState { Units = 7 };
        await opened.Snapshots.SaveSnapshotAsync(streamId, version: 3, globalSequence: 9, state);
        var (loaded, info) = await opened.Snapshots.LoadSnapshotAsync<InventoryState>(streamId);
        Assert.NotNull(info);
        Assert.Equal(3, info!.Version);
        Assert.Equal(9, info.GlobalSequence);
        Assert.Equal(7, loaded!.Units);
    }

    [ContractFact]
    public async Task SaveThenLoad_RoundTripsStateAndPlaintextTags()
    {
        var opened = await Backend.OpenSnapshotsAsync(ContractStores.Isolated());
        var tags = new[] { "product:sku-1", "warehouse:east" };
        var dcbId = DcbSnapshotId.FromLoadTags(tags);
        var state = new InventoryState { Units = 42 };
        await opened.DcbSnapshots.SaveDcbSnapshotAsync(dcbId, 17, state, loadTags: tags);
        var (restored, info) = await opened.DcbSnapshots.LoadDcbSnapshotAsync<InventoryState>(dcbId);
        Assert.NotNull(info);
        Assert.Equal(17, info!.GlobalSequence);
        Assert.Equal(42, restored!.Units);
        Assert.Equal(DcbSnapshotId.HexLength, dcbId.Length);
        if (Cap.StoresDcbSnapshotLoadTags)
        {
            var storedTags = await opened.ReadDcbLoadTagsAsync!(dcbId, CancellationToken.None);
            Assert.Equal(tags, storedTags);
        }
    }

    [ContractFact]
    public async Task CorruptSnapshot_FallsBackToFullReplay()
    {
        SkipUnless(
            Cap.CanCorruptDcbSnapshot,
            "In-memory does not expose the stored snapshot bytes to replace.");
        var opened = await Backend.OpenSnapshotsAsync(ContractStores.Isolated());
        var tags = new[] { "product:sku-corrupt" };
        var dcbId = DcbSnapshotId.FromLoadTags(tags);
        var repo = CreateRepository(opened);
        await repo.AppendWithContextAsync(
            [
                new InventoryCounted(Guid.NewGuid(), DateTime.UtcNow, 5),
                new InventoryCounted(Guid.NewGuid(), DateTime.UtcNow, 7)
            ],
            tags);
        var good = await repo.GetStateAsync<InventoryState>(tags);
        Assert.Equal(12, good.Units);
        await opened.DcbSnapshots.SaveDcbSnapshotAsync(dcbId, 2, good, loadTags: tags);
        await opened.CorruptDcbSnapshotAsync!(dcbId, CancellationToken.None);
        var (corrupt, info) = await opened.DcbSnapshots.LoadDcbSnapshotAsync<InventoryState>(dcbId);
        Assert.Null(info);
        Assert.Null(corrupt);
        Assert.Equal(12, (await repo.GetStateAsync<InventoryState>(tags)).Units);
    }

    [Fact]
    public async Task HandleCommandAsync_EventCountStrategy1_WritesHashedSnapshot_ThenReloadUsesIt()
    {
        var opened = await Backend.OpenSnapshotsAsync(ContractStores.Isolated());
        var resolver = new SnapshotStrategyResolver();
        resolver.RegisterForDcb<InventoryEntity>(new EventCountSnapshotStrategy(1));
        var queue = new SnapshotWriteQueue();
        var pump = new SnapshotWriteHostedService(queue);
        await pump.StartAsync(CancellationToken.None);
        var repo = CreateRepository(opened, resolver, queue);
        var tags = new[] { "product:sku-cmd" };
        var dcbId = DcbSnapshotId.FromLoadTags(tags);
        try
        {
            var entity = await repo.CreateEntityAsync<InventoryEntity>(tags);
            await repo.HandleCommandAsync(entity, new CountInventory(3), new CommandMetadata { UserId = "u" });
            await WaitForSnapshotAsync(opened, dcbId);
        }
        finally
        {
            await pump.StopAsync(CancellationToken.None);
        }

        if (Cap.StoresDcbSnapshotLoadTags)
            Assert.Equal(tags, await opened.ReadDcbLoadTagsAsync!(dcbId, CancellationToken.None));

        var (snapped, info) = await opened.DcbSnapshots.LoadDcbSnapshotAsync<InventoryEntity>(dcbId);
        Assert.NotNull(info);
        Assert.NotNull(snapped);
        Assert.Equal(3, snapped!.State.Units);
        var loaded = await repo.GetOrCreateEntityAsync<InventoryEntity>(tags);
        Assert.Equal(3, loaded.State.Units);
        Assert.Equal(0, loaded.EventsSinceLastSnapshot);
    }

    [Fact]
    public async Task GetStateAsync_SameHashedKey_RestoresSnapshotAndReplaysDelta()
    {
        var opened = await Backend.OpenSnapshotsAsync(ContractStores.Isolated());
        var tags = new[] { "product:sku-delta" };
        var dcbId = DcbSnapshotId.FromLoadTags(tags);
        var repo = CreateRepository(opened);
        await repo.AppendWithContextAsync(
            Enumerable.Range(0, 10).Select(i => (IEvent)new InventoryCounted(Guid.NewGuid(), DateTime.UtcNow, 1)).ToArray(),
            tags);
        var atTen = await repo.GetStateAsync<InventoryState>(tags);
        Assert.Equal(10, atTen.Units);
        var max = await opened.Store.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags(tags)));
        await opened.DcbSnapshots.SaveDcbSnapshotAsync(dcbId, max, atTen, loadTags: tags);
        await repo.AppendWithContextAsync(
            Enumerable.Range(0, 5).Select(i => (IEvent)new InventoryCounted(Guid.NewGuid(), DateTime.UtcNow, 1)).ToArray(),
            tags);
        Assert.Equal(15, (await repo.GetStateAsync<InventoryState>(tags)).Units);
    }

    private static DcbRepository CreateRepository(
        ContractSnapshots opened,
        SnapshotStrategyResolver? resolver = null,
        SnapshotWriteQueue? queue = null)
        => new(
            opened.Store,
            new TestMetadataProvider(),
            new TestTenantContextProvider(null),
            Options.Create(new LawnDartOptions { RequireTenantId = false }),
            null,
            null,
            null,
            eventSourcingOptions: Options.Create(new EventSourcingOptions { EnforceDcbTenantIsolation = false }),
            dcbSnapshotStore: opened.DcbSnapshots,
            strategyResolver: resolver,
            snapshotWriteQueue: queue);

    private static async Task WaitForSnapshotAsync(ContractSnapshots opened, string dcbId)
    {
        for (var i = 0; i < 50; i++)
        {
            var (_, info) = await opened.DcbSnapshots.LoadDcbSnapshotAsync<InventoryEntity>(dcbId);
            if (info != null)
                return;
            await Task.Delay(50);
        }

        throw new TimeoutException($"DCB snapshot for {dcbId} was not written.");
    }

    [EventTypeName("snapshot-contract.inventory-counted")]
    public sealed record InventoryCounted(Guid Id, DateTime Timestamp, int Units) : IEvent;

    public sealed class InventoryState : IState
    {
        public int Units { get; set; }
        public void Apply(InventoryCounted e) => Units += e.Units;
    }

    public sealed class CountInventory : ICommand
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public int Units { get; }
        public CountInventory(int units) => Units = units;
    }

    public sealed class InventoryEntity : DcbEntity<InventoryState>
    {
        public void Handle(CountInventory count) =>
            Emit(new InventoryCounted(Guid.NewGuid(), DateTime.UtcNow, count.Units), Tags.ToArray());

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is InventoryCounted counted)
                State.Apply(counted);
        }
    }
}
