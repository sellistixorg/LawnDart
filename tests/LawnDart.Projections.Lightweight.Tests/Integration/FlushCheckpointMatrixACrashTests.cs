using System.Text.Json;
using LawnDart.EventStore;
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
/// Matrix A crash / ordering smoke using injectable stores.
/// Plan allows a faster fake-store suite for crash ordering; SQL happy paths live in
/// <see cref="FlushCheckpointMatrixASqlTests"/>.
/// </summary>
[Trait("Category", "Integration")]
public sealed class FlushCheckpointMatrixACrashTests
{
    private const string AdditiveKey = "CounterSummary:v1";
    private const string IdempotentKey = "SetCountCounter:v1";

    private static ProjectionRegistration MakeAdditiveReg() =>
        new(
            handlerType: typeof(CounterSummaryProjection),
            viewType: typeof(CounterView),
            projectionName: "CounterSummary",
            kind: ProjectionKind.SingleStream,
            tenantScope: TenantScope.TenantScoped,
            streamType: "Counter",
            endpoint: new ProjectionEndpointAttribute("/api/views/counters/{id}", "Counter.View"));

    private static ProjectionRegistration MakeIdempotentReg() =>
        new(
            handlerType: typeof(SetCountCounterProjection),
            viewType: typeof(CounterView),
            projectionName: "SetCountCounter",
            kind: ProjectionKind.SingleStream,
            tenantScope: TenantScope.TenantScoped,
            streamType: "Counter",
            endpoint: new ProjectionEndpointAttribute("/api/views/set-counters/{id}", "Counter.View"));

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static LightweightProjectionOptions FlushOptions(
        int checkpointInterval = 1,
        bool skipTail = false,
        int catchUpThreshold = 1000) => new()
    {
        PollInterval = TimeSpan.FromMilliseconds(20),
        CheckpointInterval = checkpointInterval,
        BatchSize = 50,
        SkipTailFlushWhileCatchingUp = skipTail,
        CatchUpFlushThreshold = catchUpThreshold
    };

    private static async Task AppendCountersAsync(IEventStore store, int count, CancellationToken ct)
    {
        for (var i = 0; i < count; i++)
        {
            await store.AppendAsync(
                $"tenant1:Counter:m-{i}",
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, i + 1)],
                metadata: Meta(),
                cancellationToken: ct);
        }
    }

    private static async Task<(Dictionary<string, int> Counts, long Checkpoint)> RunOracleAsync(
        IEventStore eventStore,
        ProjectionRegistration reg,
        string storageKey,
        int eventCount,
        CancellationToken ct)
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var runner = new LightweightProjectionRunnerService(
            reg, eventStore, views, checkpoints,
            new SingleNodePartitioningService(), FlushOptions());

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, storageKey, eventCount - 1, TimeSpan.FromSeconds(30), ct);
        await runner.StopAsync(ct);
        runner.Dispose();

        var ckpt = await checkpoints.GetCheckpointAsync(storageKey, 0, ct);
        var counts = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var (id, json) in await views.GetViewsByTypeAsync(storageKey, ct))
            counts[id] = JsonSerializer.Deserialize<CounterView>(json, ProjectionViewJson.Read)!.Count;

        return (counts, ckpt!.LastSequencePosition);
    }

    private static async Task AssertMatchesOracleAsync(
        IViewStore views,
        ICheckpointStore checkpoints,
        string storageKey,
        Dictionary<string, int> oracleCounts,
        long oracleCheckpoint,
        CancellationToken ct)
    {
        var ckpt = await checkpoints.GetCheckpointAsync(storageKey, 0, ct);
        Assert.NotNull(ckpt);
        Assert.Equal(oracleCheckpoint, ckpt!.LastSequencePosition);

        foreach (var (id, expected) in oracleCounts)
        {
            var json = await views.GetViewAsync(storageKey, id, ct);
            Assert.NotNull(json);
            Assert.Equal(expected, JsonSerializer.Deserialize<CounterView>(json!, ProjectionViewJson.Read)!.Count);
        }
    }

    // ── A4 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A4_MidFlushViewFailure_CheckpointUnchanged_ThenRecovers()
    {
        var inner = new InMemoryViewStore();
        var views = new FailNSaveViewsStore(inner, failCount: 1);
        var checkpoints = new CountingCheckpointStore(new InMemoryCheckpointStore(inner));
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        await AppendCountersAsync(store, 3, cts.Token);

        var runner = new LightweightProjectionRunnerService(
            MakeAdditiveReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), FlushOptions());

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, AdditiveKey, 2, TimeSpan.FromSeconds(30), cts.Token);
        await runner.StopAsync(cts.Token);
        runner.Dispose();

        Assert.True(views.SaveViewsAttempts >= 2, "Expected at least one injected failure then retry");
        Assert.True(checkpoints.SaveCount >= 1);

        var (oracle, ckpt) = await RunOracleAsync(store, MakeAdditiveReg(), AdditiveKey, 3, cts.Token);
        await AssertMatchesOracleAsync(inner, checkpoints, AdditiveKey, oracle, ckpt, cts.Token);
    }

    [Fact]
    public async Task A4_MidBatchLoopFailure_DoesNotAdvanceCheckpointUntilFullFlush()
    {
        var inner = new InMemoryViewStore();
        var failing = new FailThenSucceedViewStore(inner, failAfter: 1);
        var views = new LoopOnlyViewStore(failing);
        var checkpoints = new InMemoryCheckpointStore(inner);
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        await AppendCountersAsync(store, 3, cts.Token);

        var runner = new LightweightProjectionRunnerService(
            MakeAdditiveReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), FlushOptions());

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, AdditiveKey, 2, TimeSpan.FromSeconds(30), cts.Token);
        await runner.StopAsync(cts.Token);
        runner.Dispose();

        Assert.True(failing.Attempts > 1);
        Assert.True(views.SaveViewsCalls >= 1);

        var (oracle, ckpt) = await RunOracleAsync(store, MakeAdditiveReg(), AdditiveKey, 3, cts.Token);
        await AssertMatchesOracleAsync(inner, checkpoints, AdditiveKey, oracle, ckpt, cts.Token);
    }

    // ── A5 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A5_KillBeforeCheckpoint_RestartReachesOracle()
    {
        // Idempotent handler: restore + re-apply from a lagging checkpoint must not corrupt.
        var reg = MakeIdempotentReg();
        var views = new InMemoryViewStore();
        var innerCkpt = new InMemoryCheckpointStore(views);
        var checkpoints = new FailNCheckpointStore(innerCkpt, failCount: 10_000);
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        await AppendCountersAsync(store, 4, cts.Token);
        var (oracle, oracleCkpt) = await RunOracleAsync(store, reg, IdempotentKey, 4, cts.Token);

        var runner1 = new LightweightProjectionRunnerService(
            reg, store, views, checkpoints,
            new SingleNodePartitioningService(), FlushOptions());
        await runner1.StartAsync(CancellationToken.None);

        await ProjectionRunnerTestHarness.WaitForViewAsync(
            views, IdempotentKey, "tenant1:Counter:m-0", TimeSpan.FromSeconds(20),
            json => JsonSerializer.Deserialize<CounterView>(json, ProjectionViewJson.Read)!.Count == 1);

        await runner1.StopAsync(cts.Token);
        runner1.Dispose();

        Assert.Null(await innerCkpt.GetCheckpointAsync(IdempotentKey, 0, cts.Token));
        Assert.True(checkpoints.SaveAttempts >= 1);

        var runner2 = new LightweightProjectionRunnerService(
            reg, store, views, innerCkpt,
            new SingleNodePartitioningService(), FlushOptions());
        await runner2.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            innerCkpt, IdempotentKey, oracleCkpt, TimeSpan.FromSeconds(30), cts.Token);
        await runner2.StopAsync(cts.Token);
        runner2.Dispose();

        await AssertMatchesOracleAsync(views, innerCkpt, IdempotentKey, oracle, oracleCkpt, cts.Token);
    }

    // ── A6 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A6_KillBeforeAnyViewWrite_RestartFromOldCheckpoint()
    {
        var inner = new InMemoryViewStore();
        var views = new FailNSaveViewsStore(inner, failCount: 3);
        var checkpoints = new InMemoryCheckpointStore(inner);
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        await AppendCountersAsync(store, 3, cts.Token);
        var (oracle, oracleCkpt) = await RunOracleAsync(store, MakeAdditiveReg(), AdditiveKey, 3, cts.Token);

        var runner1 = new LightweightProjectionRunnerService(
            MakeAdditiveReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), FlushOptions());
        await runner1.StartAsync(CancellationToken.None);
        await Task.Delay(150, cts.Token);
        await runner1.StopAsync(cts.Token);
        runner1.Dispose();

        Assert.Null(await checkpoints.GetCheckpointAsync(AdditiveKey, 0, cts.Token));
        Assert.Empty(await inner.GetViewsByTypeAsync(AdditiveKey, cts.Token));

        var runner2 = new LightweightProjectionRunnerService(
            MakeAdditiveReg(), store, inner, checkpoints,
            new SingleNodePartitioningService(), FlushOptions());
        await runner2.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, AdditiveKey, oracleCkpt, TimeSpan.FromSeconds(30), cts.Token);
        await runner2.StopAsync(cts.Token);
        runner2.Dispose();

        await AssertMatchesOracleAsync(inner, checkpoints, AdditiveKey, oracle, oracleCkpt, cts.Token);
    }

    // ── A7 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A7_KillAfterCheckpoint_WarmRestartMatchesOracle()
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        await AppendCountersAsync(store, 5, cts.Token);
        var (oracle, oracleCkpt) = await RunOracleAsync(store, MakeAdditiveReg(), AdditiveKey, 5, cts.Token);

        var runner1 = new LightweightProjectionRunnerService(
            MakeAdditiveReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), FlushOptions());
        await runner1.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, AdditiveKey, oracleCkpt, TimeSpan.FromSeconds(20), cts.Token);
        await runner1.StopAsync(cts.Token);
        runner1.Dispose();

        var runner2 = new LightweightProjectionRunnerService(
            MakeAdditiveReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), FlushOptions());
        await runner2.StartAsync(CancellationToken.None);
        await Task.Delay(100, cts.Token);
        await runner2.StopAsync(cts.Token);
        runner2.Dispose();

        await AssertMatchesOracleAsync(views, checkpoints, AdditiveKey, oracle, oracleCkpt, cts.Token);
    }

    // ── A8 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A8_CatchUpPolicy_FlushCountUpperBound()
    {
        var views = new InMemoryViewStore();
        var checkpoints = new CountingCheckpointStore(new InMemoryCheckpointStore(views));
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

        const int eventCount = 500;
        await AppendCountersAsync(store, eventCount, cts.Token);

        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(15),
            CheckpointInterval = 100,
            BatchSize = 50,
            SkipTailFlushWhileCatchingUp = true,
            CatchUpFlushThreshold = 50
        };

        var runner = new LightweightProjectionRunnerService(
            MakeAdditiveReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), options);

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, AdditiveKey, eventCount - 1, TimeSpan.FromSeconds(45), cts.Token);
        await runner.StopAsync(cts.Token);
        runner.Dispose();

        Assert.True(
            checkpoints.SaveCount <= 12,
            $"Expected catch-up to bound flushes; got {checkpoints.SaveCount} checkpoint saves");
        Assert.True(checkpoints.SaveCount >= 5, $"Expected interval flushes; got {checkpoints.SaveCount}");
    }

    // ── A12 ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A12_IdempotentReApply_ViewsAheadOfCheckpoint_NoCorruption()
    {
        var reg = MakeIdempotentReg();
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        await AppendCountersAsync(store, 4, cts.Token);
        var (oracle, oracleCkpt) = await RunOracleAsync(store, reg, IdempotentKey, 4, cts.Token);

        foreach (var (id, count) in oracle)
        {
            await views.SaveViewAsync(
                IdempotentKey, id,
                JsonSerializer.Serialize(new CounterView { Count = count }, ProjectionViewJson.Write),
                checkpoint: oracleCkpt,
                cancellationToken: cts.Token);
        }

        await checkpoints.SaveCheckpointAsync(new ProjectionCheckpoint
        {
            ProjectionType = IdempotentKey,
            NodeId = 0,
            LastSequencePosition = 0,
            LastUpdated = DateTime.UtcNow,
            TotalEventsProcessed = 1
        }, cts.Token);

        var runner = new LightweightProjectionRunnerService(
            reg, store, views, checkpoints,
            new SingleNodePartitioningService(), FlushOptions());
        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, IdempotentKey, oracleCkpt, TimeSpan.FromSeconds(30), cts.Token);
        await runner.StopAsync(cts.Token);
        runner.Dispose();

        await AssertMatchesOracleAsync(views, checkpoints, IdempotentKey, oracle, oracleCkpt, cts.Token);
    }
}
