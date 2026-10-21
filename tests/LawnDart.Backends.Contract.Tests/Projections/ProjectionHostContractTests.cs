using System.Text.Json;
using LawnDart;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Admin;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Partitioning;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Flush, rebuild, and working-set behavior shared by in-memory, SQL Server, and Postgres stores.
/// </summary>
public abstract class ProjectionHostContractTests : ContractSuite
{
    protected ProjectionHostContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [ContractFact(Timeout = 90000)]
    public async Task Flush_WritesViewsAndCheckpoint()
    {
        SkipUnless(Cap.HasProjectionStores, "This backend does not have view and checkpoint stores yet.");
        var stores = await Backend.OpenProjectionStoresAsync(ContractStores.Isolated());
        var events = NewEventStore();
        var streamId = "acct:Counter:" + Guid.NewGuid().ToString("N");
        for (var amount = 1; amount <= 5; amount++)
            await events.AppendAsync(streamId, [Increment(amount)], metadata: Meta());

        var head = await events.GetCurrentSequenceAsync();
        var reg = CounterRegistration();
        var runner = NewRunner(reg, events, stores, Options());
        await runner.StartAsync(CancellationToken.None);
        try
        {
            await WaitForCheckpointAsync(stores.Checkpoints, reg.StorageKey, head);
            var json = await stores.Views.GetViewAsync(reg.StorageKey, streamId);
            AssertViewJson(CountJson(15), json);
            Assert.Equal(head, (await stores.Checkpoints.GetCheckpointAsync(reg.StorageKey, 0))!.LastSequencePosition);
        }
        finally
        {
            await runner.StopAsync(CancellationToken.None);
            runner.Dispose();
        }
    }

    [ContractFact(Timeout = 90000)]
    public async Task Rebuild_DeletesRowsThenRebuilds()
    {
        SkipUnless(Cap.HasProjectionStores, "This backend does not have view and checkpoint stores yet.");
        var stores = await Backend.OpenProjectionStoresAsync(ContractStores.Isolated());
        var events = NewEventStore();
        var streamId = "acct:Counter:" + Guid.NewGuid().ToString("N");
        for (var amount = 1; amount <= 5; amount++)
            await events.AppendAsync(streamId, [Increment(amount)], metadata: Meta());

        var reg = CounterRegistration();
        var options = Options();
        var first = NewRunner(reg, events, stores, options);
        await first.StartAsync(CancellationToken.None);
        await WaitForCheckpointAsync(stores.Checkpoints, reg.StorageKey, await events.GetCurrentSequenceAsync());
        await first.StopAsync(CancellationToken.None);
        first.Dispose();

        Assert.NotNull(await stores.Views.GetViewAsync(reg.StorageKey, streamId));
        await stores.Views.DeleteAllViewsAsync(reg.StorageKey);
        await stores.Checkpoints.DeleteCheckpointAsync(reg.StorageKey);
        Assert.Null(await stores.Views.GetViewAsync(reg.StorageKey, streamId));
        Assert.Null(await stores.Checkpoints.GetCheckpointAsync(reg.StorageKey, 0));

        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => NewRunner(r, events, stores, options));
        var admin = new BoundedContextProjectionAdmin(
            manager,
            stores.Checkpoints,
            stores.Views,
            new ProjectionRegistrationCatalog([reg]),
            new SingleNodePartitioningService());
        try
        {
            await admin.RebuildAsync("ContractCounter");
            await WaitForCheckpointAsync(stores.Checkpoints, reg.StorageKey, await events.GetCurrentSequenceAsync());
            AssertViewJson(CountJson(15), await stores.Views.GetViewAsync(reg.StorageKey, streamId));
        }
        finally
        {
            await manager.StopAllAsync(CancellationToken.None);
        }
    }

    [ContractFact(Timeout = 90000)]
    public async Task LazyWorkingSet_HydratesOnlyTheTouchedStream()
    {
        SkipUnless(Cap.HasProjectionStores, "This backend does not have view and checkpoint stores yet.");
        var stores = await Backend.OpenProjectionStoresAsync(ContractStores.Isolated());
        var events = NewEventStore();
        var streams = Enumerable.Range(0, 3)
            .Select(_ => "acct:Counter:" + Guid.NewGuid().ToString("N"))
            .ToArray();
        foreach (var streamId in streams)
            await events.AppendAsync(streamId, [Increment(4)], metadata: Meta());

        var reg = CounterRegistration();
        var eager = NewRunner(reg, events, stores, Options(ProjectionWorkingSetMode.EagerRestore));
        await eager.StartAsync(CancellationToken.None);
        await WaitForCheckpointAsync(stores.Checkpoints, reg.StorageKey, await events.GetCurrentSequenceAsync());
        await eager.StopAsync(CancellationToken.None);
        eager.Dispose();

        var lazy = NewRunner(reg, events, stores, Options(ProjectionWorkingSetMode.Lazy));
        await lazy.StartAsync(CancellationToken.None);
        try
        {
            Assert.Equal(0, lazy.WorkingSetCount);
            await events.AppendAsync(streams[0], [Increment(1)], metadata: Meta());
            await WaitForCheckpointAsync(stores.Checkpoints, reg.StorageKey, await events.GetCurrentSequenceAsync());
            Assert.Equal(1, lazy.WorkingSetCount);
            Assert.True(lazy.TryGetView(streams[0], out var json, out _));
            AssertViewJson(CountJson(5), json);
        }
        finally
        {
            await lazy.StopAsync(CancellationToken.None);
            lazy.Dispose();
        }
    }

    private static InMemoryEventStore NewEventStore()
        => new(options: new InMemoryEventStoreOptions { RequireTenantId = false });

    private static LightweightProjectionRunnerService NewRunner(
        ProjectionRegistration registration,
        IEventStore events,
        ContractProjectionStores stores,
        LightweightProjectionOptions options)
        => new(
            registration,
            events,
            stores.Views,
            stores.Checkpoints,
            new SingleNodePartitioningService(),
            options);

    private static ProjectionRegistration CounterRegistration()
        => new(
            typeof(ContractCounterProjection),
            typeof(ContractCounterView),
            "ContractCounter",
            ProjectionKind.SingleStream,
            TenantScope.TenantScoped,
            streamType: "Counter");

    private static LightweightProjectionOptions Options(ProjectionWorkingSetMode workingSet = ProjectionWorkingSetMode.EagerRestore)
        => new()
        {
            PollInterval = TimeSpan.FromMilliseconds(20),
            CheckpointInterval = 1,
            BatchSize = 50,
            SkipTailFlushWhileCatchingUp = false,
            UseSubscribe = false,
            WorkingSetMode = workingSet
        };

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static ContractCounterIncremented Increment(int amount)
        => new(Guid.NewGuid(), DateTime.UtcNow, amount);

    private static string CountJson(int count)
        => JsonSerializer.Serialize(new ContractCounterView { Count = count }, ProjectionViewJson.Write);

    private static async Task WaitForCheckpointAsync(
        LawnDart.Projections.Checkpoints.ICheckpointStore checkpoints,
        string storageKey,
        long position)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (!cts.IsCancellationRequested)
        {
            var checkpoint = await checkpoints.GetCheckpointAsync(storageKey, 0, CancellationToken.None);
            if (checkpoint is not null && checkpoint.LastSequencePosition >= position)
                return;
            await Task.Delay(25, cts.Token);
        }
    }
}

[EventTypeName("backend-contract.counter-incremented")]
public sealed record ContractCounterIncremented(Guid Id, DateTime Timestamp, int Amount) : IEvent;

public sealed class ContractCounterView
{
    public int Count { get; set; }
}

public sealed class ContractCounterProjection : LawnDart.Projections.Sdk.ProjectionBase<ContractCounterView>
{
    public void Handle(ContractCounterIncremented e) => State.Count += e.Amount;
}
