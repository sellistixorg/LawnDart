using System.Diagnostics;
using System.Text.Json;
using LawnDart;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration.Delivery;

/// <summary>
/// Fixed-N catch-up layers used to split P: Subscribe drain vs Handle/flush vs
/// read-cache JSON vs shared-pipe fan-out vs per-runner typed Subscribe.
/// </summary>
internal static class CatchUpCostHarness
{
    public const int CiTickCount = 5_000;
    public const string TickCountEnv = "LAWNDART_CATCHUP_COST_N";
    public const string ProgressKey = "CatchUpProgress:v1";

    public static int ResolveTickCount()
    {
        var raw = Environment.GetEnvironmentVariable(TickCountEnv);
        return int.TryParse(raw, out var n) && n > 0 ? n : CiTickCount;
    }

    public static ProjectionRegistration[] ProgressOnlyRegs() =>
        [ProjectionScanner.TryBuildRegistration(typeof(CatchUpProgressProjection))!];

    public static ProjectionRegistration[] ProgressPlusNoopRegs() =>
    [
        ProjectionScanner.TryBuildRegistration(typeof(CatchUpProgressProjection))!,
        ProjectionScanner.TryBuildRegistration(typeof(CatchUpNoop1Projection))!,
        ProjectionScanner.TryBuildRegistration(typeof(CatchUpNoop2Projection))!,
        ProjectionScanner.TryBuildRegistration(typeof(CatchUpNoop3Projection))!,
        ProjectionScanner.TryBuildRegistration(typeof(CatchUpNoop4Projection))!
    ];

    public static LightweightProjectionOptions ChaosLikeOptions(
        bool sharedSubscribe,
        ProjectionReadCacheMode cacheMode) =>
        new()
        {
            BatchSize = 1000,
            CheckpointInterval = 500,
            PollInterval = TimeSpan.FromMilliseconds(200),
            SkipTailFlushWhileCatchingUp = true,
            CatchUpFlushThreshold = 1000,
            UseSubscribe = true,
            UseSharedSubscribe = sharedSubscribe,
            ReadCacheMode = cacheMode
        };

    public static async Task AppendTicksAsync(IEventStore store, int count, EventMetadata meta)
    {
        const int batchSize = 1000;
        var now = new DateTime(2026, 8, 18, 21, 0, 0, DateTimeKind.Utc);
        for (var i = 0; i < count; i += batchSize)
        {
            var take = Math.Min(batchSize, count - i);
            var batch = new List<IEvent>(take);
            for (var j = 0; j < take; j++)
                batch.Add(new CatchUpTick(Guid.NewGuid(), now, 0));
            await store.AppendAsync("CatchUp:lane:0", batch, metadata: meta);
        }
    }

    public static async Task<CatchUpCostRow> MeasureSubscribeDrainAsync(
        int count,
        TimeSpan timeout)
    {
        EventTypeNameResolver.Warmup([typeof(CatchUpTick), typeof(CatchUpShopNoise)]);
        var store = new InMemoryEventStore();
        var meta = new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "catchup-cost" };
        await AppendTicksAsync(store, count, meta);
        var head = await store.GetCurrentSequenceAsync();

        var filter = EventSubscriptionFilter.ForQuery(
            Query.FromItems(QueryItem.ByType("CatchUpTick")));
        var sw = Stopwatch.StartNew();
        await using var handle = store.Subscribe("catchup-drain", fromSequence: 0, filter);
        var read = await DrainExactAsync(handle, count, timeout);
        sw.Stop();

        return new CatchUpCostRow(
            Layer: "SubscribeDrain",
            Head: head,
            Events: count,
            Enumerated: read,
            HandleProgress: 0,
            HandleNoop: 0,
            FlushWaves: 0,
            Pms: sw.Elapsed.TotalMilliseconds);
    }

    public static async Task<CatchUpCostRow> MeasureRunnerAsync(
        string layer,
        int count,
        bool sharedSubscribe,
        ProjectionReadCacheMode cacheMode,
        bool includeNoopShops,
        TimeSpan timeout)
    {
        EventTypeNameResolver.Warmup([typeof(CatchUpTick), typeof(CatchUpShopNoise)]);
        CatchUpCostCounters.Reset();

        var inner = new InMemoryEventStore();
        var store = new CountingEventStore(inner);
        var views = new CountingViewStore(new InMemoryViewStore());
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = ChaosLikeOptions(sharedSubscribe, cacheMode);
        var cache = new ProjectionReadCache(options);
        var meta = new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "catchup-cost" };

        await AppendTicksAsync(store, count, meta);
        var head = await store.GetCurrentSequenceAsync();

        var regs = includeNoopShops ? ProgressPlusNoopRegs() : ProgressOnlyRegs();
        var manager = ProjectionDeliveryFixture.CreateManager(regs, store, views, checkpoints, options, cache);

        store.ResetEnumerated();
        var sw = Stopwatch.StartNew();
        try
        {
            await manager.StartAllAsync();
            await WaitUntilProgressCaughtUpAsync(manager, head, timeout);
            sw.Stop();

            var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
                views,
                ProgressKey,
                ProjectionInstanceIds.UnpartitionedBase,
                timeout,
                json => JsonSerializer.Deserialize<CatchUpTickView>(json, ProjectionViewJson.Read)?.Ticks == count);
            if (viewJson is null)
            {
                throw new TimeoutException(
                    $"{layer}: view {ProgressKey} did not reach {count} ticks within {timeout}.");
            }

            return new CatchUpCostRow(
                Layer: layer,
                Head: head,
                Events: count,
                Enumerated: store.EventsEnumerated,
                HandleProgress: Interlocked.Read(ref CatchUpCostCounters.Progress),
                HandleNoop: Interlocked.Read(ref CatchUpCostCounters.Noop),
                FlushWaves: views.FlushWaves,
                Pms: sw.Elapsed.TotalMilliseconds);
        }
        finally
        {
            await manager.StopAllAsync();
        }
    }

    public static IReadOnlyList<CatchUpCostRow> AssertCorrectness(IReadOnlyList<CatchUpCostRow> rows, int count)
    {
        foreach (var row in rows)
        {
            if (row.Layer == "SubscribeDrain")
            {
                if (row.Enumerated != count)
                    throw new InvalidOperationException($"{row.Layer}: drained {row.Enumerated}, expected {count}.");
                continue;
            }

            if (row.HandleProgress != count)
                throw new InvalidOperationException(
                    $"{row.Layer}: progress Handle={row.HandleProgress}, expected {count}.");
            if (row.HandleNoop != 0)
                throw new InvalidOperationException(
                    $"{row.Layer}: noop Handle={row.HandleNoop}, expected 0.");
        }

        return rows;
    }

    public static string FormatTable(IReadOnlyList<CatchUpCostRow> rows)
    {
        var drain = rows.FirstOrDefault(r => r.Layer == "SubscribeDrain");
        var drainEps = drain.Events > 0 && drain.Pms > 0 ? drain.Events * 1000.0 / drain.Pms : 0;

        var lines = new List<string>
        {
            "Catch-up P cost split (do not mix with W)",
            $"N={rows[0].Events}  |  EPS vs SubscribeDrain in last column",
            "Layer | enumerated | Handle(progress/noop) | flushes | P ms | EPS | vs drain"
        };

        foreach (var row in rows)
        {
            var eps = row.Pms > 0 ? row.Events * 1000.0 / row.Pms : 0;
            var vs = drainEps > 0 ? eps / drainEps : 0;
            lines.Add(
                $"{row.Layer} | {row.Enumerated} | {row.HandleProgress}/{row.HandleNoop} | {row.FlushWaves} | {row.Pms:F1} | {eps:F0} | {vs:P0}");
        }

        return string.Join(Environment.NewLine, lines);
    }

    private static async Task<int> DrainExactAsync(ISubscriptionHandle handle, int count, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        var read = 0;
        while (read < count)
        {
            if (!await handle.Events.WaitToReadAsync(cts.Token).ConfigureAwait(false))
                throw new TimeoutException($"Subscribe drain completed after {read}/{count}.");
            while (read < count && handle.Events.TryRead(out _))
                read++;
        }

        return read;
    }

    private static async Task WaitUntilProgressCaughtUpAsync(
        BoundedContextProjectionRunnerManager manager,
        long head,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!cts.IsCancellationRequested)
        {
            var snap = manager.GetSnapshot(ProgressKey);
            if ((snap?.LastAppliedSequence ?? -1) >= head)
                return;

            try
            {
                await Task.Delay(5, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        var last = manager.GetSnapshot(ProgressKey)?.LastAppliedSequence;
        throw new TimeoutException(
            $"CatchUpProgress lastApplied={last} did not reach H={head} within {timeout}.");
    }
}

internal readonly record struct CatchUpCostRow(
    string Layer,
    long Head,
    int Events,
    long Enumerated,
    long HandleProgress,
    long HandleNoop,
    long FlushWaves,
    double Pms);
