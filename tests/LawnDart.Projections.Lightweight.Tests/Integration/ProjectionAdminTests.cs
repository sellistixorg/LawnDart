using Microsoft.Extensions.DependencyInjection;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Admin;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Acceptance tests for projection rebuild administration.
/// </summary>
[Trait("Category", "Integration")]
public sealed class ProjectionAdminTests
{
    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static LightweightProjectionOptions AggressiveOptions => new()
    {
        PollInterval       = TimeSpan.FromMilliseconds(10),
        CheckpointInterval = 1,
        BatchSize          = 50
    };

    private static ProjectionRegistration CounterReg() =>
        ProjectionScanner.Scan([typeof(CounterSummaryProjection).Assembly])
            .First(r => r.StorageKey.Contains("CounterSummary"));

    private static ProjectionRegistration GlobalTagReg() =>
        ProjectionScanner.Scan([typeof(GlobalTagIndexProjection).Assembly])
            .First(r => r.StorageKey.Contains("GlobalTagIndex"));

    private static (BoundedContextProjectionRunnerManager manager, BoundedContextProjectionAdmin admin)
        MakeManagerAndAdmin(
            IReadOnlyList<ProjectionRegistration> regs,
            IEventStore eventStore,
            IViewStore viewStore,
            ICheckpointStore checkpoints,
            IPartitioningService? partitioning = null)
    {
        var partition = partitioning ?? new SingleNodePartitioningService();
        var mgr = new BoundedContextProjectionRunnerManager(
            regs,
            reg => new LightweightProjectionRunnerService(
                reg, eventStore, viewStore, checkpoints, partition, AggressiveOptions));
        var admin = new BoundedContextProjectionAdmin(
            mgr, checkpoints, viewStore,
            new ProjectionRegistrationCatalog(regs),
            partition);
        return (mgr, admin);
    }

    // ── Ordering safety: concurrent appends during rebuild ────────────────────

    /// <summary>
    /// Events appended while the runner is stopped (during <c>RebuildAsync</c>) must not be
    /// lost — the cold restart replays from 0 and picks them all up.
    /// </summary>
    [Fact]
    public async Task OrderingSafety_ConcurrentAppendsNotLost_AfterRebuild()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        var reg        = CounterReg();
        var eventStore = new InMemoryEventStore();
        var views      = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var streamId   = $"Counter:{Guid.NewGuid()}";

        // Seed 5 events before the first run
        for (var i = 1; i <= 5; i++)
            await eventStore.AppendAsync(streamId,
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: Meta(), cancellationToken: cts.Token);

        var (manager, admin) = MakeManagerAndAdmin([reg], eventStore, views, checkpoints);

        // Start all runners to reach a steady state first
        await manager.StartAllAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, reg.StorageKey, 4, TimeSpan.FromSeconds(30));

        // Append 10 more events concurrently with RebuildAsync
        var concurrentAppends = Task.Run(async () =>
        {
            for (var i = 0; i < 10; i++)
            {
                await eventStore.AppendAsync(streamId,
                    [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                    metadata: Meta(), cancellationToken: cts.Token);
                await Task.Delay(5, cts.Token);
            }
        }, cts.Token);

        await admin.RebuildAsync("CounterSummary", ct: cts.Token);
        await concurrentAppends;

        // Wait for the fresh runner to process all 15 events (positions 1..15)
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, reg.StorageKey, 15, TimeSpan.FromSeconds(30));

        var viewJson = await views.GetViewAsync(reg.StorageKey, streamId, cts.Token);
        Assert.NotNull(viewJson);
        var view = System.Text.Json.JsonSerializer.Deserialize<CounterView>(viewJson!, ProjectionViewJson.Read);
        Assert.Equal(15, view!.Count);

        await manager.StopAllAsync(CancellationToken.None);
    }

    // ── Runner manager: stop one, others keep consuming ───────────────────────

    /// <summary>
    /// Stopping one projection via <see cref="IProjectionRunnerManager.StopAsync"/> must not
    /// affect other projections.  Restarting it via <see cref="IProjectionRunnerManager.StartAsync"/>
    /// must catch up.
    /// </summary>
    [Fact]
    public async Task RunnerManager_StopOneProjOthersKeepConsuming_RestartCatchesUp()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        var counterReg = CounterReg();
        var globalReg  = GlobalTagReg();
        var regs       = new[] { counterReg, globalReg };

        var eventStore  = new InMemoryEventStore();
        var views       = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);

        var (manager, _) = MakeManagerAndAdmin(regs, eventStore, views, checkpoints);
        await manager.StartAllAsync(CancellationToken.None);

        // Append one Counter event (processed by CounterSummary)
        var streamId = $"Counter:{Guid.NewGuid()}";
        await eventStore.AppendAsync(streamId,
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 7)],
            metadata: Meta(), cancellationToken: cts.Token);

        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, counterReg.StorageKey, 0, TimeSpan.FromSeconds(30));

        // Stop only CounterSummary
        await manager.StopAsync(counterReg.StorageKey, cts.Token);
        Assert.False(manager.IsRunning(counterReg.StorageKey));
        Assert.True(manager.IsRunning(globalReg.StorageKey));

        // Append more counter events (CounterSummary is stopped, GlobalTagIndex still running)
        for (var i = 0; i < 3; i++)
            await eventStore.AppendAsync(streamId,
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: Meta(), cancellationToken: cts.Token);

        // Append a global event (GlobalTagIndex should consume it while CounterSummary is down)
        await eventStore.AppendAsync("Global:tag",
            [new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "test-tag")],
            metadata: Meta(), cancellationToken: cts.Token);

        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, globalReg.StorageKey, 4, TimeSpan.FromSeconds(30));

        // Verify CounterSummary has NOT advanced past its pre-stop checkpoint
        var counterCp = await checkpoints.GetCheckpointAsync(counterReg.StorageKey, 0, cts.Token);
        // The runner was stopped after seq 0; seq 1-3 were appended while stopped
        Assert.True(counterCp is null || counterCp.LastSequencePosition < 4,
            "CounterSummary should not have consumed events appended while stopped");

        // Restart CounterSummary — fresh runner, catches up from 0
        await manager.StartAsync(counterReg.StorageKey, CancellationToken.None);
        Assert.True(manager.IsRunning(counterReg.StorageKey));

        // Wait for all 4 counter events (positions 1-4) to be processed so count == 10
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, counterReg.StorageKey, 4, TimeSpan.FromSeconds(30));

        var viewJson = await views.GetViewAsync(counterReg.StorageKey, streamId, cts.Token);
        var view = System.Text.Json.JsonSerializer.Deserialize<CounterView>(viewJson!, ProjectionViewJson.Read);
        Assert.Equal(10, view!.Count); // 7 + 1 + 1 + 1 = 10

        await manager.StopAllAsync(CancellationToken.None);
    }

    // ── Keyed isolation: rebuild A never touches B ───────────────────────────

    /// <summary>
    /// Calling <see cref="IProjectionAdmin.RebuildAsync"/> on context A must not delete views or
    /// checkpoints that belong to context B.
    /// </summary>
    [Fact]
    public async Task KeyedIsolation_RebuildContextA_DoesNotTouchContextB()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        var reg         = CounterReg();
        var streamId    = $"Counter:{Guid.NewGuid()}";

        // Context A and context B each have independent stores
        var esA         = new InMemoryEventStore();
        var viewsA      = new InMemoryViewStore();
        var checkpointsA = new InMemoryCheckpointStore(viewsA);

        var esB         = new InMemoryEventStore();
        var viewsB      = new InMemoryViewStore();
        var checkpointsB = new InMemoryCheckpointStore(viewsB);

        // Seed the same events in both stores
        for (var i = 0; i < 3; i++)
        {
            await esA.AppendAsync(streamId,
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: Meta(), cancellationToken: cts.Token);
            await esB.AppendAsync(streamId,
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: Meta(), cancellationToken: cts.Token);
        }

        // Run projections in both contexts to steady state
        var (managerA, adminA) = MakeManagerAndAdmin([reg], esA, viewsA, checkpointsA);
        var (managerB, _)      = MakeManagerAndAdmin([reg], esB, viewsB, checkpointsB);

        await managerA.StartAllAsync(CancellationToken.None);
        await managerB.StartAllAsync(CancellationToken.None);

        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpointsA, reg.StorageKey, 2, TimeSpan.FromSeconds(30));
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpointsB, reg.StorageKey, 2, TimeSpan.FromSeconds(30));

        // Capture B's state before A's rebuild
        var cpBBefore = await checkpointsB.GetCheckpointAsync(reg.StorageKey, 0, cts.Token);
        var viewBBefore = await viewsB.GetViewAsync(reg.StorageKey, streamId, cts.Token);
        Assert.NotNull(cpBBefore);
        Assert.NotNull(viewBBefore);

        // Rebuild context A only
        await adminA.RebuildAsync("CounterSummary", ct: cts.Token);

        // Context B's stores must be completely untouched
        var cpBAfter    = await checkpointsB.GetCheckpointAsync(reg.StorageKey, 0, cts.Token);
        var viewBAfter  = await viewsB.GetViewAsync(reg.StorageKey, streamId, cts.Token);

        Assert.NotNull(cpBAfter);
        Assert.Equal(cpBBefore!.LastSequencePosition, cpBAfter!.LastSequencePosition);
        Assert.Equal(viewBBefore, viewBAfter);

        // Context A's runner re-ran and re-built
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpointsA, reg.StorageKey, 2, TimeSpan.FromSeconds(30));

        await managerA.StopAllAsync(CancellationToken.None);
        await managerB.StopAllAsync(CancellationToken.None);
    }

    // ── Multi-node interlock ─────────────────────────────────────────────────

    /// <summary>
    /// <see cref="IProjectionAdmin.RebuildAsync"/> must throw <see cref="NotSupportedException"/>
    /// when <c>TotalInstances &gt; 1</c>, and must leave views and checkpoints completely untouched.
    /// </summary>
    [Fact]
    public async Task MultiNodeInterlock_ThrowsBeforeTouchingStores()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        var reg        = CounterReg();
        var streamId   = $"Counter:{Guid.NewGuid()}";
        var eventStore = new InMemoryEventStore();
        var views      = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);

        // Seed and run to steady state
        for (var i = 0; i < 3; i++)
            await eventStore.AppendAsync(streamId,
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: Meta(), cancellationToken: cts.Token);

        var singleNode = new SingleNodePartitioningService();
        var (manager, _) = MakeManagerAndAdmin([reg], eventStore, views, checkpoints, singleNode);
        await manager.StartAllAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, reg.StorageKey, 2, TimeSpan.FromSeconds(30));
        await manager.StopAllAsync(CancellationToken.None);

        // Capture store state
        var cpBefore   = await checkpoints.GetCheckpointAsync(reg.StorageKey, 0, cts.Token);
        var viewBefore = await views.GetViewAsync(reg.StorageKey, streamId, cts.Token);
        Assert.NotNull(cpBefore);
        Assert.NotNull(viewBefore);

        // Wire an admin that uses a multi-node partitioning service
        var multiNode = new ConsistentHashPartitioningService(nodeInstance: 0, totalInstances: 2);
        var adminMulti = new BoundedContextProjectionAdmin(
            manager, checkpoints, views,
            new ProjectionRegistrationCatalog([reg]),
            multiNode);

        // Must throw before touching stores
        var ex = await Assert.ThrowsAsync<NotSupportedException>(() =>
            adminMulti.RebuildAsync("CounterSummary", ct: cts.Token));
        Assert.Contains("TotalInstances", ex.Message);
        Assert.Contains("2", ex.Message);

        // Checkpoint and view must be exactly as before
        var cpAfter   = await checkpoints.GetCheckpointAsync(reg.StorageKey, 0, cts.Token);
        var viewAfter = await views.GetViewAsync(reg.StorageKey, streamId, cts.Token);

        Assert.Equal(cpBefore!.LastSequencePosition, cpAfter!.LastSequencePosition);
        Assert.Equal(viewBefore, viewAfter);
        Assert.True(manager.IsRunning(reg.StorageKey) == false,
            "Runner must remain stopped — interlock threw before StopAsync was called (or stop was no-op since it was already stopped)");
    }

    // ── Interlock: IsRunning still false when called while running ──────────────

    [Fact]
    public async Task MultiNodeInterlock_WhileRunning_ThrowsBeforeAnyStateChange()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        var reg        = CounterReg();
        var streamId   = $"Counter:{Guid.NewGuid()}";
        var eventStore = new InMemoryEventStore();
        var views      = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);

        for (var i = 0; i < 2; i++)
            await eventStore.AppendAsync(streamId,
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: Meta(), cancellationToken: cts.Token);

        var singleNode = new SingleNodePartitioningService();
        var mgr = new BoundedContextProjectionRunnerManager(
            [reg],
            r => new LightweightProjectionRunnerService(r, eventStore, views, checkpoints, singleNode, AggressiveOptions));

        await mgr.StartAllAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, reg.StorageKey, 1, TimeSpan.FromSeconds(30));

        var cpBefore   = await checkpoints.GetCheckpointAsync(reg.StorageKey, 0, cts.Token);
        var viewBefore = await views.GetViewAsync(reg.StorageKey, streamId, cts.Token);

        var multiNode = new ConsistentHashPartitioningService(0, 2);
        var adminMulti = new BoundedContextProjectionAdmin(
            mgr, checkpoints, views, new ProjectionRegistrationCatalog([reg]), multiNode);

        await Assert.ThrowsAsync<NotSupportedException>(() =>
            adminMulti.RebuildAsync("CounterSummary", ct: cts.Token));

        // Views and checkpoints must be untouched
        var cpAfter   = await checkpoints.GetCheckpointAsync(reg.StorageKey, 0, cts.Token);
        var viewAfter = await views.GetViewAsync(reg.StorageKey, streamId, cts.Token);
        Assert.Equal(cpBefore!.LastSequencePosition, cpAfter!.LastSequencePosition);
        Assert.Equal(viewBefore, viewAfter);

        await mgr.StopAllAsync(CancellationToken.None);
    }
}
