using System.Diagnostics.Metrics;
using System.Net;
using System.Text.Json;
using LawnDart.EventSourcing.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Telemetry;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Matrix C SQL smoke — lazy restore / clean eviction / hydrate / GET after eviction.
/// C5 (100k load) is intentionally on-demand and omitted here.
/// </summary>
[Collection("SqlFlushMatrixA")]
[Trait("Category", "Integration")]
public sealed class WorkingSetMatrixCSqlTests
{
    private readonly MsSqlMatrixAFixture _fx;

    public WorkingSetMatrixCSqlTests(MsSqlMatrixAFixture fx) => _fx = fx;

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    // ── C1 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task C1_LazyRestore_LowInitialWorkingSet_HydratesOnEvent()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_c1", cts.Token);
        var store = new InMemoryEventStore();

        for (var i = 0; i < 5; i++)
        {
            await store.AppendAsync(
                $"tenant1:Counter:c1-{i}",
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 10 + i)],
                metadata: Meta(),
                cancellationToken: cts.Token);
        }

        var head1 = await store.GetCurrentSequenceAsync(cts.Token);
        var eager = new LightweightProjectionRunnerService(
            MatrixBceSqlHost.MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(),
            MatrixBceSqlHost.AggressiveOptions(workingSet: ProjectionWorkingSetMode.EagerRestore));
        await eager.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, MatrixBceSqlHost.CounterStorageKey, head1, TimeSpan.FromSeconds(30), cts.Token);
        await eager.StopAsync(cts.Token);
        eager.Dispose();

        var lazy = new LightweightProjectionRunnerService(
            MatrixBceSqlHost.MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(),
            MatrixBceSqlHost.AggressiveOptions(
                cacheMode: ProjectionReadCacheMode.Off,
                workingSet: ProjectionWorkingSetMode.Lazy));
        await lazy.StartAsync(CancellationToken.None);
        await Task.Delay(100, cts.Token);
        Assert.Equal(0, lazy.WorkingSetCount);

        await store.AppendAsync(
            "tenant1:Counter:c1-0",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
            metadata: Meta(),
            cancellationToken: cts.Token);
        var head2 = await store.GetCurrentSequenceAsync(cts.Token);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, MatrixBceSqlHost.CounterStorageKey, head2, TimeSpan.FromSeconds(30), cts.Token);

        Assert.Equal(1, lazy.WorkingSetCount);
        Assert.True(lazy.TryGetView("tenant1:Counter:c1-0", out var json, out _));
        Assert.Equal(11, JsonSerializer.Deserialize<CounterView>(json, ProjectionViewJson.Read)!.Count);

        await lazy.StopAsync(cts.Token);
        lazy.Dispose();
    }

    // ── C2 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task C2_EvictClean_NeverEvictsDirty_BoundsWorkingSet()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_c2", cts.Token);
        var store = new InMemoryEventStore();
        const int n = 8;

        for (var i = 0; i < n; i++)
        {
            await store.AppendAsync(
                $"tenant1:Counter:c2-{i}",
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, i + 1)],
                metadata: Meta(),
                cancellationToken: cts.Token);
        }

        var head = await store.GetCurrentSequenceAsync(cts.Token);
        var runner = new LightweightProjectionRunnerService(
            MatrixBceSqlHost.MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(),
            MatrixBceSqlHost.AggressiveOptions(
                cacheMode: ProjectionReadCacheMode.Off,
                workingSet: ProjectionWorkingSetMode.EagerRestore,
                workingSetMax: 3));

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, MatrixBceSqlHost.CounterStorageKey, head, TimeSpan.FromSeconds(40), cts.Token);
        await ProjectionRunnerTestHarness.WaitForWorkingSetAtMostAsync(
            runner, maxCount: 3, TimeSpan.FromSeconds(10));
        Assert.Equal(n, (await views.GetViewsByTypeAsync(MatrixBceSqlHost.CounterStorageKey, cts.Token)).Count());

        await runner.StopAsync(cts.Token);
        runner.Dispose();
    }

    // ── C3 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task C3_EventAfterEviction_HydratesFromSql_AndApplies()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_c3", cts.Token);
        var store = new InMemoryEventStore();

        await store.AppendAsync(
            "tenant1:Counter:c3-keep",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
            metadata: Meta(), cancellationToken: cts.Token);
        await store.AppendAsync(
            "tenant1:Counter:c3-evict",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 2)],
            metadata: Meta(), cancellationToken: cts.Token);

        var head1 = await store.GetCurrentSequenceAsync(cts.Token);
        var runner = new LightweightProjectionRunnerService(
            MatrixBceSqlHost.MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(),
            MatrixBceSqlHost.AggressiveOptions(
                cacheMode: ProjectionReadCacheMode.Off,
                workingSet: ProjectionWorkingSetMode.Lazy,
                workingSetMax: 1));

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, MatrixBceSqlHost.CounterStorageKey, head1, TimeSpan.FromSeconds(30), cts.Token);
        await ProjectionRunnerTestHarness.WaitForWorkingSetAtMostAsync(
            runner, maxCount: 1, TimeSpan.FromSeconds(10));

        await store.AppendAsync(
            "tenant1:Counter:c3-evict",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 3)],
            metadata: Meta(), cancellationToken: cts.Token);
        var head2 = await store.GetCurrentSequenceAsync(cts.Token);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, MatrixBceSqlHost.CounterStorageKey, head2, TimeSpan.FromSeconds(30), cts.Token);

        var durable = await views.GetViewWithCheckpointAsync(
            MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:c3-evict", cts.Token);
        Assert.NotNull(durable);
        Assert.Equal(5, JsonSerializer.Deserialize<CounterView>(durable!.Value.ViewData, ProjectionViewJson.Read)!.Count);

        await runner.StopAsync(cts.Token);
        runner.Dispose();
    }

    // ── C4 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task C4_GetAfterEviction_ReturnsSql200()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_c4", cts.Token);
        var store = new InMemoryEventStore();
        const int n = 6;

        for (var i = 0; i < n; i++)
        {
            await store.AppendAsync(
                $"tenant1:Counter:c4-{i}",
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, i + 1)],
                metadata: Meta(), cancellationToken: cts.Token);
        }

        var head = await store.GetCurrentSequenceAsync(cts.Token);
        var options = MatrixBceSqlHost.AggressiveOptions(
            cacheMode: ProjectionReadCacheMode.Off,
            workingSet: ProjectionWorkingSetMode.EagerRestore,
            workingSetMax: 2);
        var reg = MatrixBceSqlHost.MakeCounterReg();
        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => new LightweightProjectionRunnerService(
                r, store, views, checkpoints, new SingleNodePartitioningService(), options));

        await manager.StartAsync(MatrixBceSqlHost.CounterStorageKey, CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, MatrixBceSqlHost.CounterStorageKey, head, TimeSpan.FromSeconds(40), cts.Token);

        // Pick an instance not in the runner working set (if any); SQL must still serve it.
        string? coldId = null;
        for (var i = 0; i < n; i++)
        {
            var id = $"tenant1:Counter:c4-{i}";
            if (!manager.TryGetView(MatrixBceSqlHost.CounterStorageKey, id, out _, out _))
            {
                coldId = $"c4-{i}";
                break;
            }
        }

        Assert.NotNull(coldId);

        var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(views, [reg], manager: manager);
        await using (app)
        {
            var response = await MatrixBceSqlHost.GetAsync(client, $"/api/views/counters/{coldId}");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(
                ProjectionFreshness.ReadSourceSql,
                response.Headers.GetValues(ProjectionFreshness.ReadSourceHeader).Single());
        }

        await manager.StopAllAsync(cts.Token);
    }

    // ── C6 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task C6_EvictionMetricsToggle_SilentWhenDisabled_ReadsUnaffected()
    {
        var previous = ProjectionTelemetry.Options;
        ProjectionTelemetry.Configure(new ProjectionTelemetryOptions { EnableExtendedMetrics = false });
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
            var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_c6", cts.Token);
            var store = new InMemoryEventStore();

            for (var i = 0; i < 5; i++)
            {
                await store.AppendAsync(
                    $"tenant1:Counter:c6-{i}",
                    [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                    metadata: Meta(), cancellationToken: cts.Token);
            }

            using var listener = new MetricSumListener("projections.workingset.evictions");

            var head = await store.GetCurrentSequenceAsync(cts.Token);
            var runner = new LightweightProjectionRunnerService(
                MatrixBceSqlHost.MakeCounterReg(), store, views, checkpoints,
                new SingleNodePartitioningService(),
                MatrixBceSqlHost.AggressiveOptions(
                    cacheMode: ProjectionReadCacheMode.Off,
                    workingSet: ProjectionWorkingSetMode.EagerRestore,
                    workingSetMax: 2));

            await runner.StartAsync(CancellationToken.None);
            await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
                checkpoints, MatrixBceSqlHost.CounterStorageKey, head, TimeSpan.FromSeconds(30), cts.Token);
            await ProjectionRunnerTestHarness.WaitForWorkingSetAtMostAsync(
                runner, maxCount: 2, TimeSpan.FromSeconds(10));
            Assert.Equal(0, listener.GetSum("projections.workingset.evictions"));
            Assert.Equal(5, (await views.GetViewsByTypeAsync(MatrixBceSqlHost.CounterStorageKey, cts.Token)).Count());

            await runner.StopAsync(cts.Token);
            runner.Dispose();
        }
        finally
        {
            ProjectionTelemetry.Configure(previous);
        }
    }

    private sealed class MetricSumListener : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly Dictionary<string, long> _sums = new(StringComparer.Ordinal);

        public MetricSumListener(params string[] names)
        {
            foreach (var n in names)
                _sums[n] = 0;

            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == ProjectionTelemetry.MeterName && _sums.ContainsKey(instrument.Name))
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((inst, measurement, _, _) =>
            {
                if (_sums.ContainsKey(inst.Name))
                    _sums[inst.Name] += measurement;
            });
            _listener.Start();
        }

        public long GetSum(string name) => _sums.TryGetValue(name, out var v) ? v : 0;
        public void Dispose() => _listener.Dispose();
    }
}
