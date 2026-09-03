using System.Net;
using System.Text.Json;
using LawnDart.EventSourcing.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Matrix D — barriered write-behind. Uses in-memory stores + flush doubles;
/// durability barrier is identical to the SQL path.
/// </summary>
[Trait("Category", "Integration")]
public sealed class WriteBehindMatrixDTests
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
            endpoint: new ProjectionEndpointAttribute("/api/views/counters/{counterId}", "Counter.View"));

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static LightweightProjectionOptions WriteBehindOptions(bool enabled = true) => new()
    {
        PollInterval = TimeSpan.FromMilliseconds(15),
        CheckpointInterval = 1,
        BatchSize = 50,
        SkipTailFlushWhileCatchingUp = false,
        EnableWriteBehind = enabled,
        WriteBehindMaxInFlight = 1,
        ReadCacheMode = ProjectionReadCacheMode.Hot
    };

    // ── D1 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task D1_DrainThenCheckpoint_CheckpointNeverLeadsDurableViews()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store = new InMemoryEventStore();
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);

        for (var i = 0; i < 5; i++)
        {
            await store.AppendAsync(
                $"tenant1:Counter:d1-{i}",
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, i + 1)],
                metadata: Meta(),
                cancellationToken: cts.Token);
        }

        var head = await store.GetCurrentSequenceAsync(cts.Token);
        var runner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), WriteBehindOptions(),
            readCache: cache);

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, StorageKey, head, TimeSpan.FromSeconds(20), cts.Token);
        await runner.StopAsync(cts.Token);
        runner.Dispose();

        var ckpt = await checkpoints.GetCheckpointAsync(StorageKey, 0, cts.Token);
        Assert.NotNull(ckpt);
        Assert.Equal(head, ckpt!.LastSequencePosition);
        Assert.Equal(5, (await views.GetViewsByTypeAsync(StorageKey, cts.Token)).Count());

        foreach (var (id, _) in await views.GetViewsByTypeAsync(StorageKey, cts.Token))
        {
            var row = await views.GetViewWithCheckpointAsync(StorageKey, id, cts.Token);
            Assert.NotNull(row);
            // Per-instance applied seq is durable and ≤ global head; checkpoint did not skip views.
            Assert.True(row!.Value.Checkpoint <= ckpt.LastSequencePosition);
        }
    }

    // ── D2 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task D2_KillMidDrain_CheckpointDoesNotLead_RestartSafe()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var inner = new InMemoryViewStore();
        // Keep failing for the whole first runner lifetime — simulates kill mid-drain.
        var views = new FailNSaveViewsStore(inner, failCount: 10_000);
        var checkpoints = new InMemoryCheckpointStore(inner);
        var store = new InMemoryEventStore();

        await store.AppendAsync(
            "tenant1:Counter:d2",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 3)],
            metadata: Meta(),
            cancellationToken: cts.Token);

        var head = await store.GetCurrentSequenceAsync(cts.Token);
        var runner1 = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), WriteBehindOptions());

        await runner1.StartAsync(CancellationToken.None);

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline && views.SaveViewsAttempts < 1)
            await Task.Delay(25, cts.Token);

        Assert.True(views.SaveViewsAttempts >= 1, "Expected at least one failed drain attempt");
        await Task.Delay(150, cts.Token);

        var mid = await checkpoints.GetCheckpointAsync(StorageKey, 0, cts.Token);
        Assert.Null(mid); // checkpoint must not advance when view drain never acks
        Assert.Null(await inner.GetViewAsync(StorageKey, "tenant1:Counter:d2", cts.Token));

        await runner1.StopAsync(cts.Token);
        runner1.Dispose();

        Assert.Null(await checkpoints.GetCheckpointAsync(StorageKey, 0, cts.Token));

        // Healthy restart from old ckpt — catch-up writes durable views and advances checkpoint.
        var runner2 = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, inner, checkpoints,
            new SingleNodePartitioningService(), WriteBehindOptions(enabled: false));
        await runner2.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, StorageKey, head, TimeSpan.FromSeconds(20), cts.Token);
        await runner2.StopAsync(cts.Token);
        runner2.Dispose();

        var after = await inner.GetViewWithCheckpointAsync(StorageKey, "tenant1:Counter:d2", cts.Token);
        Assert.NotNull(after);
        Assert.Equal(3, JsonSerializer.Deserialize<CounterView>(after!.Value.ViewData, ProjectionViewJson.Read)!.Count);
    }

    // ── D3 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task D3_GetDuringDrain_MemoryCurrent_MinSequenceProtectsSqlLag()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var durable = new InMemoryViewStore();
        var delayed = new DelayedSaveViewsStore(durable, TimeSpan.FromMilliseconds(600));
        var checkpoints = new InMemoryCheckpointStore(durable);
        var store = new InMemoryEventStore();
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);
        var reg = MakeCounterReg();

        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => new LightweightProjectionRunnerService(
                r, store, delayed, checkpoints, new SingleNodePartitioningService(),
                WriteBehindOptions(),
                readCache: cache));

        await store.AppendAsync(
            "tenant1:Counter:d3",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 9)],
            metadata: Meta(),
            cancellationToken: cts.Token);

        await manager.StartAsync(StorageKey, CancellationToken.None);
        await delayed.FlushEntered.WaitAsync(cts.Token);

        // During drain: durable may still be empty; memory GET must be current.
        Assert.Null(await durable.GetViewAsync(StorageKey, "tenant1:Counter:d3", cts.Token));

        var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(durable, [reg], cache, manager);
        await using (app)
        {
            var mem = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/d3");
            Assert.Equal(HttpStatusCode.OK, mem.StatusCode);
            Assert.Equal(
                ProjectionFreshness.ReadSourceMemory,
                mem.Headers.GetValues(ProjectionFreshness.ReadSourceHeader).Single());
            var body = await mem.Content.ReadAsStringAsync(cts.Token);
            Assert.Equal(9, JsonSerializer.Deserialize<CounterView>(body,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!.Count);
        }

        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, StorageKey,
            await store.GetCurrentSequenceAsync(cts.Token),
            TimeSpan.FromSeconds(15), cts.Token);
        await manager.StopAllAsync(cts.Token);

        // Second apply with flushes failing → SQL lags; minSequence fail-closed on SQL path.
        var failing = new FailNSaveViewsStore(durable, failCount: 10_000);
        var manager2 = new BoundedContextProjectionRunnerManager(
            [reg],
            r => new LightweightProjectionRunnerService(
                r, store, failing, checkpoints, new SingleNodePartitioningService(),
                WriteBehindOptions(),
                readCache: cache));

        await store.AppendAsync(
            "tenant1:Counter:d3",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
            metadata: Meta(),
            cancellationToken: cts.Token);
        await manager2.StartAsync(StorageKey, CancellationToken.None);

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (manager2.TryGetView(StorageKey, "tenant1:Counter:d3", out var j, out _)
                && JsonSerializer.Deserialize<CounterView>(j, ProjectionViewJson.Read)!.Count == 10)
                break;
            await Task.Delay(25, cts.Token);
        }

        var (app2, client2) = await MatrixBceSqlHost.StartQueryHostAsync(durable, [reg], cache, manager2);
        await using (app2)
        {
            var ok = await MatrixBceSqlHost.GetAsync(client2, "/api/views/counters/d3");
            Assert.Equal(HttpStatusCode.OK, ok.StatusCode);

            await manager2.StopAllAsync(cts.Token);
            cache.Clear(StorageKey);

            var head2 = await store.GetCurrentSequenceAsync(cts.Token);
            var rejected = await MatrixBceSqlHost.GetAsync(
                client2, $"/api/views/counters/d3?minSequence={head2}");
            Assert.Equal(HttpStatusCode.Conflict, rejected.StatusCode);
        }
    }

    // ── D4 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task D4_FeatureDefaultOff_FlushAwaitsDurableWrite()
    {
        Assert.False(new LightweightProjectionOptions().EnableWriteBehind);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var inner = new InMemoryViewStore();
        var delayed = new DelayedSaveViewsStore(inner, TimeSpan.FromMilliseconds(250));
        var checkpoints = new InMemoryCheckpointStore(inner);
        var store = new InMemoryEventStore();

        await store.AppendAsync(
            "tenant1:Counter:d4",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 2)],
            metadata: Meta(),
            cancellationToken: cts.Token);

        var head = await store.GetCurrentSequenceAsync(cts.Token);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var runner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, delayed, checkpoints,
            new SingleNodePartitioningService(), WriteBehindOptions(enabled: false));

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, StorageKey, head, TimeSpan.FromSeconds(20), cts.Token);
        sw.Stop();

        Assert.True(delayed.SaveViewsCalls >= 1);
        Assert.NotNull(await inner.GetViewAsync(StorageKey, "tenant1:Counter:d4", cts.Token));
        Assert.True(sw.ElapsedMilliseconds >= 200,
            $"Expected inline flush to await ~250ms delay; elapsed {sw.ElapsedMilliseconds}ms");

        await runner.StopAsync(cts.Token);
        runner.Dispose();
    }
}
