using System.Text.Json;
using LawnDart.EventSourcing.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Lazy restore, clean-only eviction, hydrate after eviction.
/// </summary>
[Trait("Category", "Integration")]
public sealed class WorkingSetEvictionTests
{
    private const string StorageKey = "CounterSummary:v1";

    private static ProjectionRegistration MakeCounterReg() =>
        new(
            handlerType: typeof(CounterSummaryProjection),
            viewType: typeof(CounterView),
            projectionName: "CounterSummary",
            kind: ProjectionKind.SingleStream,
            tenantScope: TenantScope.TenantScoped,
            streamType: "Counter",
            endpoint: new ProjectionEndpointAttribute("/api/views/counters/{id}", "Counter.View"));

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    [Fact]
    public async Task LazyMode_SkipsBulkRestore_HydratesOnFirstEvent()
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        // Pass 1: eager catch-up so durable + checkpoint are warm.
        for (var i = 0; i < 5; i++)
        {
            await store.AppendAsync(
                $"tenant1:Counter:seed-{i}",
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 10 + i)],
                metadata: Meta(),
                cancellationToken: cts.Token);
        }

        var head1 = await store.GetCurrentSequenceAsync(cts.Token);
        var eager = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(),
            new LightweightProjectionOptions
            {
                PollInterval = TimeSpan.FromMilliseconds(15),
                CheckpointInterval = 1,
                WorkingSetMode = ProjectionWorkingSetMode.EagerRestore,
                SkipTailFlushWhileCatchingUp = false
            });

        await eager.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, StorageKey, head1, TimeSpan.FromSeconds(20), cts.Token);
        await eager.StopAsync(cts.Token);
        eager.Dispose();

        Assert.Equal(5, (await views.GetViewsByTypeAsync(StorageKey, cts.Token)).Count());

        // Pass 2: Lazy restart must not bulk-load the 5 durable views.
        var lazy = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(),
            new LightweightProjectionOptions
            {
                PollInterval = TimeSpan.FromMilliseconds(15),
                CheckpointInterval = 1,
                WorkingSetMode = ProjectionWorkingSetMode.Lazy,
                WorkingSetMaxInstances = 0,
                SkipTailFlushWhileCatchingUp = false
            });

        await lazy.StartAsync(CancellationToken.None);
        await Task.Delay(80, cts.Token);
        Assert.Equal(0, lazy.WorkingSetCount);

        // New event → hydrate seed-0 (count 10) then apply +1 → 11.
        await store.AppendAsync(
            "tenant1:Counter:seed-0",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
            metadata: Meta(),
            cancellationToken: cts.Token);

        var head2 = await store.GetCurrentSequenceAsync(cts.Token);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, StorageKey, head2, TimeSpan.FromSeconds(20), cts.Token);

        Assert.Equal(1, lazy.WorkingSetCount);
        Assert.True(lazy.TryGetView("tenant1:Counter:seed-0", out var json, out _));
        Assert.Equal(11, JsonSerializer.Deserialize<CounterView>(json, ProjectionViewJson.Read)!.Count);

        await lazy.StopAsync(cts.Token);
        lazy.Dispose();
    }

    [Fact]
    public async Task EvictClean_NeverEvictsDirty_AndBoundsWorkingSet()
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        const int n = 8;
        for (var i = 0; i < n; i++)
        {
            await store.AppendAsync(
                $"tenant1:Counter:ws-{i}",
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, i + 1)],
                metadata: Meta(),
                cancellationToken: cts.Token);
        }

        var head = await store.GetCurrentSequenceAsync(cts.Token);
        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(15),
            CheckpointInterval = 1000, // flush mainly via opportunistic/stop
            BatchSize = 100,
            WorkingSetMode = ProjectionWorkingSetMode.EagerRestore,
            WorkingSetMaxInstances = 3,
            SkipTailFlushWhileCatchingUp = false
        };

        var runner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), options);

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, StorageKey, head, TimeSpan.FromSeconds(30), cts.Token);
        await ProjectionRunnerTestHarness.WaitForWorkingSetAtMostAsync(
            runner, maxCount: 3, TimeSpan.FromSeconds(10));
        var durable = (await views.GetViewsByTypeAsync(StorageKey, cts.Token)).ToList();
        Assert.Equal(n, durable.Count);

        await runner.StopAsync(cts.Token);
        runner.Dispose();
    }

    [Fact]
    public async Task EventAfterEviction_HydratesFromDurable_AndAppliesCorrectly()
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        // Two instances; max=1 forces eviction of the colder one after flush.
        await store.AppendAsync(
            "tenant1:Counter:keep",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
            metadata: Meta(),
            cancellationToken: cts.Token);
        await store.AppendAsync(
            "tenant1:Counter:evict-me",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 2)],
            metadata: Meta(),
            cancellationToken: cts.Token);

        var head1 = await store.GetCurrentSequenceAsync(cts.Token);
        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(15),
            CheckpointInterval = 1,
            BatchSize = 50,
            WorkingSetMode = ProjectionWorkingSetMode.Lazy,
            WorkingSetMaxInstances = 1,
            SkipTailFlushWhileCatchingUp = false
        };

        var runner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), options);

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, StorageKey, head1, TimeSpan.FromSeconds(20), cts.Token);
        await ProjectionRunnerTestHarness.WaitForWorkingSetAtMostAsync(
            runner, maxCount: 1, TimeSpan.FromSeconds(10));

        // Touch "keep" via TryGetView if present; append to the likely-evicted stream.
        // Regardless of which survived, appending to evict-me must hydrate + apply.
        await store.AppendAsync(
            "tenant1:Counter:evict-me",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 3)],
            metadata: Meta(),
            cancellationToken: cts.Token);

        var head2 = await store.GetCurrentSequenceAsync(cts.Token);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, StorageKey, head2, TimeSpan.FromSeconds(20), cts.Token);

        var durable = await views.GetViewWithCheckpointAsync(
            StorageKey, "tenant1:Counter:evict-me", cts.Token);
        Assert.NotNull(durable);
        Assert.Equal(5, JsonSerializer.Deserialize<CounterView>(durable!.Value.ViewData, ProjectionViewJson.Read)!.Count);

        await runner.StopAsync(cts.Token);
        runner.Dispose();
    }

    [Fact]
    public async Task EagerRestore_ThenEvict_TrimsWorkingSetOnStartup()
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        for (var i = 0; i < 6; i++)
        {
            await views.SaveViewAsync(
                StorageKey, $"tenant1:Counter:e-{i}",
                JsonSerializer.Serialize(new CounterView { Count = 1 }, ProjectionViewJson.Write),
                checkpoint: i,
                cancellationToken: cts.Token);
        }

        await checkpoints.SaveCheckpointAsync(new ProjectionCheckpoint
        {
            ProjectionType = StorageKey,
            NodeId = 0,
            LastSequencePosition = 5,
            LastUpdated = DateTime.UtcNow,
            TotalEventsProcessed = 6
        }, cts.Token);

        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(50),
            CheckpointInterval = 500,
            WorkingSetMode = ProjectionWorkingSetMode.EagerRestore,
            WorkingSetMaxInstances = 2,
            SkipTailFlushWhileCatchingUp = true
        };

        var runner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), options);

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForWorkingSetAtMostAsync(
            runner, maxCount: 2, TimeSpan.FromSeconds(10));

        await runner.StopAsync(cts.Token);
        runner.Dispose();
    }
}
