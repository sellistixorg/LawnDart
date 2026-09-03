using System.Diagnostics.Metrics;
using System.Net;
using System.Text.Json;
using LawnDart.EventSourcing.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Admin;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;
using LawnDart.Projections.Telemetry;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Matrix B SQL smoke — reads / freshness / hot cache against Testcontainers SQL Server.
/// </summary>
[Collection("SqlFlushMatrixA")]
[Trait("Category", "Integration")]
public sealed class HotPathMatrixBSqlTests
{
    private readonly MsSqlMatrixAFixture _fx;

    public HotPathMatrixBSqlTests(MsSqlMatrixAFixture fx) => _fx = fx;

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    // ── B1 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task B1_MemoryHitAfterApply_GetReturnsCurrentWithMemorySource()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_b1", cts.Token);
        var store = new InMemoryEventStore();
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);
        var options = MatrixBceSqlHost.AggressiveOptions();

        var reg = MatrixBceSqlHost.MakeCounterReg();
        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => new LightweightProjectionRunnerService(
                r, store, views, checkpoints, new SingleNodePartitioningService(), options,
                readCache: cache));

        await store.AppendAsync(
            "tenant1:Counter:b1",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 7)],
            metadata: Meta(),
            cancellationToken: cts.Token);

        await manager.StartAsync(MatrixBceSqlHost.CounterStorageKey, CancellationToken.None);

        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline
               && !manager.TryGetView(MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:b1", out _, out _))
            await Task.Delay(25, cts.Token);

        Assert.True(
            manager.TryGetView(MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:b1", out _, out var liveSeq),
            "Expected runner memory after apply");

        var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(views, [reg], cache, manager);
        await using (app)
        {
            var response = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/b1");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(
                ProjectionFreshness.ReadSourceMemory,
                response.Headers.GetValues(ProjectionFreshness.ReadSourceHeader).Single());
            Assert.True(long.Parse(response.Headers.GetValues(ProjectionFreshness.SequenceHeader).Single()) >= liveSeq);

            var body = await response.Content.ReadAsStringAsync(cts.Token);
            var view = JsonSerializer.Deserialize<CounterView>(body,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            Assert.Equal(7, view!.Count);
        }

        await manager.StopAllAsync(cts.Token);
    }

    // ── B2 / B3 ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task B2_B3_SqlFallback_NeverFalse404_WhenDurableHasRow()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_b2", cts.Token);
        var store = new InMemoryEventStore();
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);

        await store.AppendAsync(
            "tenant1:Counter:b2",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 11)],
            metadata: Meta(),
            cancellationToken: cts.Token);

        var head = await store.GetCurrentSequenceAsync(cts.Token);
        var runner = new LightweightProjectionRunnerService(
            MatrixBceSqlHost.MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), MatrixBceSqlHost.AggressiveOptions(),
            readCache: cache);
        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, MatrixBceSqlHost.CounterStorageKey, head, TimeSpan.FromSeconds(30), cts.Token);
        await runner.StopAsync(cts.Token);
        runner.Dispose();

        // Empty accelerators — GET must still hit SQL (never false 404).
        cache.Clear(MatrixBceSqlHost.CounterStorageKey);

        var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(
            views, [MatrixBceSqlHost.MakeCounterReg()], cache);
        await using (app)
        {
            var response = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/b2");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(
                ProjectionFreshness.ReadSourceSql,
                response.Headers.GetValues(ProjectionFreshness.ReadSourceHeader).Single());

            var body = await response.Content.ReadAsStringAsync(cts.Token);
            var view = JsonSerializer.Deserialize<CounterView>(body,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            Assert.Equal(11, view!.Count);
        }
    }

    // ── B4 / B5 ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task B4_B5_MinSequence_409Then200_AgainstSql()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var (views, _) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_b45", cts.Token);

        await views.SaveViewAsync(
            MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:b45",
            JsonSerializer.Serialize(new CounterView { Count = 3 }, ProjectionViewJson.Write),
            checkpoint: 50, cancellationToken: cts.Token);

        var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(
            views, [MatrixBceSqlHost.MakeCounterReg()]);
        await using (app)
        {
            var stale = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/b45?minSequence=60");
            Assert.Equal(HttpStatusCode.Conflict, stale.StatusCode);
            var staleBody = await stale.Content.ReadAsStringAsync(cts.Token);
            Assert.DoesNotContain("\"Count\":3", staleBody, StringComparison.Ordinal);
            using (var doc = JsonDocument.Parse(staleBody))
            {
                Assert.Equal(ProjectionFreshness.NotCaughtUpCode, doc.RootElement.GetProperty("code").GetString());
            }

            var ok = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/b45?minSequence=50");
            Assert.Equal(HttpStatusCode.OK, ok.StatusCode);
            Assert.Equal("50", ok.Headers.GetValues(ProjectionFreshness.SequenceHeader).Single());
        }
    }

    // ── B6 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task B6_CacheOverwrite_NeverReturnsFirstVersionAfterSecondApply()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_b6", cts.Token);
        var store = new InMemoryEventStore();
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);
        var options = MatrixBceSqlHost.AggressiveOptions();
        var reg = MatrixBceSqlHost.MakeCounterReg();

        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => new LightweightProjectionRunnerService(
                r, store, views, checkpoints, new SingleNodePartitioningService(), options,
                readCache: cache));

        await store.AppendAsync(
            "tenant1:Counter:b6",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
            metadata: Meta(),
            cancellationToken: cts.Token);
        await manager.StartAsync(MatrixBceSqlHost.CounterStorageKey, CancellationToken.None);

        var head1 = await store.GetCurrentSequenceAsync(cts.Token);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, MatrixBceSqlHost.CounterStorageKey, head1, TimeSpan.FromSeconds(20), cts.Token);

        await store.AppendAsync(
            "tenant1:Counter:b6",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 2)],
            metadata: Meta(),
            cancellationToken: cts.Token);
        var head2 = await store.GetCurrentSequenceAsync(cts.Token);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, MatrixBceSqlHost.CounterStorageKey, head2, TimeSpan.FromSeconds(20), cts.Token);

        var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(views, [reg], cache, manager);
        await using (app)
        {
            var response = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/b6");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var body = await response.Content.ReadAsStringAsync(cts.Token);
            var view = JsonSerializer.Deserialize<CounterView>(body,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            Assert.Equal(3, view!.Count); // 1+2 — never the first-only version
            Assert.NotEqual(1, view.Count);
        }

        await manager.StopAllAsync(cts.Token);
    }

    // ── B7 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task B7_RebuildPurge_DoesNotServePreRebuildBody()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_b7", cts.Token);
        var store = new InMemoryEventStore();
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);
        var options = MatrixBceSqlHost.AggressiveOptions();
        var reg = MatrixBceSqlHost.MakeCounterReg();

        // Seed a durable + cache ghost that rebuild must remove.
        await views.SaveViewAsync(
            MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:gone",
            JsonSerializer.Serialize(new CounterView { Count = 999 }, ProjectionViewJson.Write),
            checkpoint: 9, cancellationToken: cts.Token);
        cache.Set(MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:gone",
            JsonSerializer.Serialize(new CounterView { Count = 999 }), 9);

        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => new LightweightProjectionRunnerService(
                r, store, views, checkpoints, new SingleNodePartitioningService(), options,
                readCache: cache));

        var catalog = new ProjectionRegistrationCatalog([reg]);
        var admin = new BoundedContextProjectionAdmin(
            manager, checkpoints, views, catalog, new SingleNodePartitioningService(),
            readCache: cache);

        await admin.RebuildAsync("CounterSummary", ct: cts.Token);

        Assert.False(cache.TryGet(MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:gone", out _));
        Assert.Null(await views.GetViewAsync(
            MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:gone", cts.Token));

        var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(views, [reg], cache, manager);
        await using (app)
        {
            var response = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/gone");
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        }

        await manager.StopAllAsync(cts.Token);
    }

    // ── B8 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task B8_ConcurrentGetDuringFlush_NoFalse404_SeqMonotonic()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var (sqlViews, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_b8", cts.Token);
        var delayed = new DelayedSaveViewsStore(sqlViews, TimeSpan.FromMilliseconds(400));
        var store = new InMemoryEventStore();
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);
        var options = MatrixBceSqlHost.AggressiveOptions();
        var reg = MatrixBceSqlHost.MakeCounterReg();

        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => new LightweightProjectionRunnerService(
                r, store, delayed, checkpoints, new SingleNodePartitioningService(), options,
                readCache: cache));

        await store.AppendAsync(
            "tenant1:Counter:b8",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 4)],
            metadata: Meta(),
            cancellationToken: cts.Token);

        await manager.StartAsync(MatrixBceSqlHost.CounterStorageKey, CancellationToken.None);
        await delayed.FlushEntered.WaitAsync(cts.Token);

        var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(sqlViews, [reg], cache, manager);
        await using (app)
        {
            long? prevSeq = null;
            var tasks = Enumerable.Range(0, 20).Select(async _ =>
            {
                var response = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/b8");
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                var seq = long.Parse(response.Headers.GetValues(ProjectionFreshness.SequenceHeader).Single());
                var body = await response.Content.ReadAsStringAsync();
                var view = JsonSerializer.Deserialize<CounterView>(body,
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                Assert.Equal(4, view!.Count);
                return seq;
            });

            var seqs = await Task.WhenAll(tasks);
            foreach (var seq in seqs.OrderBy(s => s))
            {
                if (prevSeq is not null)
                    Assert.True(seq >= prevSeq.Value, "Sequence must be monotonic per instance");
                prevSeq = seq;
            }
        }

        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, MatrixBceSqlHost.CounterStorageKey,
            await store.GetCurrentSequenceAsync(cts.Token),
            TimeSpan.FromSeconds(30), cts.Token);

        await manager.StopAllAsync(cts.Token);
    }

    // ── B9 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task B9_HotPromotion_RepeatedSqlGetEventuallyHitsMemory()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var (views, _) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_b9", cts.Token);
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot, promoteAfterHits: 1);

        await views.SaveViewAsync(
            MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:b9",
            JsonSerializer.Serialize(new CounterView { Count = 5 }, ProjectionViewJson.Write),
            checkpoint: 12, cancellationToken: cts.Token);

        var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(
            views, [MatrixBceSqlHost.MakeCounterReg()], cache);
        await using (app)
        {
            var first = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/b9");
            Assert.Equal(HttpStatusCode.OK, first.StatusCode);
            Assert.Equal(
                ProjectionFreshness.ReadSourceSql,
                first.Headers.GetValues(ProjectionFreshness.ReadSourceHeader).Single());

            var second = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/b9");
            Assert.Equal(HttpStatusCode.OK, second.StatusCode);
            Assert.Equal(
                ProjectionFreshness.ReadSourceMemory,
                second.Headers.GetValues(ProjectionFreshness.ReadSourceHeader).Single());
        }
    }

    // ── B10 ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task B10_CacheMetricsOff_ReadsStillCorrect()
    {
        var previous = ProjectionTelemetry.Options;
        ProjectionTelemetry.Configure(new ProjectionTelemetryOptions { EnableCacheMetrics = false });
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            var (views, _) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_b10", cts.Token);
            var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);

            await views.SaveViewAsync(
                MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:b10",
                JsonSerializer.Serialize(new CounterView { Count = 2 }, ProjectionViewJson.Write),
                checkpoint: 3, cancellationToken: cts.Token);

            using var listener = new MetricSumListener(
                "projections.read.total",
                "projections.read.cache.hit",
                "projections.read.cache.miss");

            var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(
                views, [MatrixBceSqlHost.MakeCounterReg()], cache);
            await using (app)
            {
                var response = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/b10");
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                var body = await response.Content.ReadAsStringAsync(cts.Token);
                var view = JsonSerializer.Deserialize<CounterView>(body,
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                Assert.Equal(2, view!.Count);
            }

            Assert.Equal(0, listener.GetSum("projections.read.total"));
            Assert.Equal(0, listener.GetSum("projections.read.cache.hit"));
            Assert.Equal(0, listener.GetSum("projections.read.cache.miss"));
        }
        finally
        {
            ProjectionTelemetry.Configure(previous);
        }
    }

    // ── B11 ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task B11_LagVisibility_MemoryCurrent_SqlMayLag_MinSequenceProtects()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var (sqlViews, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_b11", cts.Token);
        var store = new InMemoryEventStore();
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);
        var options = MatrixBceSqlHost.AggressiveOptions();
        var reg = MatrixBceSqlHost.MakeCounterReg();

        // First event flushes successfully.
        await store.AppendAsync(
            "tenant1:Counter:b11",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
            metadata: Meta(),
            cancellationToken: cts.Token);

        var head1 = await store.GetCurrentSequenceAsync(cts.Token);
        var warm = new LightweightProjectionRunnerService(
            reg, store, sqlViews, checkpoints, new SingleNodePartitioningService(), options,
            readCache: cache);
        await warm.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, MatrixBceSqlHost.CounterStorageKey, head1, TimeSpan.FromSeconds(20), cts.Token);
        await warm.StopAsync(cts.Token);
        warm.Dispose();

        var durableAfterFirst = await sqlViews.GetViewWithCheckpointAsync(
            MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:b11", cts.Token);
        Assert.NotNull(durableAfterFirst);
        var durableSeq = durableAfterFirst!.Value.Checkpoint;

        // Second apply with flushes failing — memory ahead of SQL.
        var failing = new FailNSaveViewsStore(sqlViews, failCount: 10_000);
        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => new LightweightProjectionRunnerService(
                r, store, failing, checkpoints, new SingleNodePartitioningService(), options,
                readCache: cache));

        await store.AppendAsync(
            "tenant1:Counter:b11",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 10)],
            metadata: Meta(),
            cancellationToken: cts.Token);

        await manager.StartAsync(MatrixBceSqlHost.CounterStorageKey, CancellationToken.None);

        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            if (manager.TryGetView(MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:b11", out var j, out _)
                && JsonSerializer.Deserialize<CounterView>(j, ProjectionViewJson.Read)!.Count == 11)
                break;
            await Task.Delay(25, cts.Token);
        }

        var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(sqlViews, [reg], cache, manager);
        await using (app)
        {
            var mem = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/b11");
            Assert.Equal(HttpStatusCode.OK, mem.StatusCode);
            Assert.Equal(
                ProjectionFreshness.ReadSourceMemory,
                mem.Headers.GetValues(ProjectionFreshness.ReadSourceHeader).Single());
            var memBody = await mem.Content.ReadAsStringAsync(cts.Token);
            Assert.Equal(11, JsonSerializer.Deserialize<CounterView>(memBody,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!.Count);

            // Force SQL path: stop runner + clear hot cache.
            await manager.StopAllAsync(cts.Token);
            cache.Clear(MatrixBceSqlHost.CounterStorageKey);

            var sql = await MatrixBceSqlHost.GetAsync(client, "/api/views/counters/b11");
            Assert.Equal(HttpStatusCode.OK, sql.StatusCode);
            Assert.Equal(
                ProjectionFreshness.ReadSourceSql,
                sql.Headers.GetValues(ProjectionFreshness.ReadSourceHeader).Single());
            var sqlSeq = long.Parse(sql.Headers.GetValues(ProjectionFreshness.SequenceHeader).Single());
            Assert.Equal(durableSeq, sqlSeq);

            var sqlBody = await sql.Content.ReadAsStringAsync(cts.Token);
            Assert.Equal(1, JsonSerializer.Deserialize<CounterView>(sqlBody,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!.Count);

            var head2 = await store.GetCurrentSequenceAsync(cts.Token);
            var rejected = await MatrixBceSqlHost.GetAsync(
                client, $"/api/views/counters/b11?minSequence={head2}");
            Assert.Equal(HttpStatusCode.Conflict, rejected.StatusCode);
        }
    }

    // ── B12 ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task B12_UnauthenticatedGet_Returns401()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var (views, _) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_b12", cts.Token);
        await views.SaveViewAsync(
            MatrixBceSqlHost.CounterStorageKey, "tenant1:Counter:b12",
            JsonSerializer.Serialize(new CounterView { Count = 1 }, ProjectionViewJson.Write),
            checkpoint: 1, cancellationToken: cts.Token);

        var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(
            views, [MatrixBceSqlHost.MakeCounterReg()]);
        await using (app)
        {
            var response = await MatrixBceSqlHost.GetAsync(
                client, "/api/views/counters/b12", authenticated: false);
            Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        }
    }

    private sealed class MetricSumListener : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly Dictionary<string, long> _sums = new(StringComparer.Ordinal);
        private readonly HashSet<string> _names;

        public MetricSumListener(params string[] instrumentNames)
        {
            _names = new HashSet<string>(instrumentNames, StringComparer.Ordinal);
            foreach (var name in _names)
                _sums[name] = 0;

            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == ProjectionTelemetry.MeterName
                    && _names.Contains(instrument.Name))
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
