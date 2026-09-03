using System.Diagnostics;
using System.Text.Json;
using Xunit.Abstractions;
using LawnDart.EventStore;
using LawnDart.EventSourcing.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration.Delivery;

/// <summary>
/// Delivery harness: manager shared Subscribe pipe, library default batch/poll,
/// fixed seed, gold-view correctness. Performance is recorded, not asserted.
/// </summary>
[Collection(ProjectionDeliveryCounterCollection.Name)]
[Trait("Category", "Delivery")]
[Trait("Category", "Integration")]
public sealed class ProjectionDeliveryFixture
{
    public const int CiNoiseCount = 20_000;
    public const int Slice0NoiseCount = 200_000;
    public const int MatchCount = 1_000;

    private readonly ITestOutputHelper _output;

    public ProjectionDeliveryFixture(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task CiSlice_20k_noise_gold_views_match()
    {
        var row = await RunSliceAsync(CiNoiseCount, MatchCount, TimeSpan.FromSeconds(30));
        AssertGold(row);
        WriteResultsRow("CI 20k", row);
    }

    [Fact]
    public async Task Slice0_200k_records_baseline()
    {
        var row = await RunSliceAsync(Slice0NoiseCount, MatchCount, TimeSpan.FromMinutes(4));
        AssertGold(row);
        Assert.True(
            row.Enumerated < row.Head,
            $"Live filters should not enumerate the full log × runners ({row.Enumerated} vs H={row.Head}).");
        Assert.True(
            row.Enumerated <= 1200,
            $"Shared pipe should enumerate ~1000 unique matches, not 3× ({row.Enumerated}).");
        WriteResultsRow("InMemory (Slice 0)", row);
    }

    [Fact]
    public async Task Rebuild_from_zero_matches_gold()
    {
        var row = await RunRebuildAsync(CiNoiseCount, MatchCount, TimeSpan.FromSeconds(30));
        AssertGold(row);
        WriteResultsRow("Rebuild from 0 (CI 20k)", row);
    }

    private async Task<DeliveryRunRow> RunSliceAsync(int noiseCount, int matchCount, TimeSpan timeout)
    {
        DeliveryHandleCounters.Reset();
        var spec = DeliverySliceSpec.Build(noiseCount, matchCount);

        var innerStore = new InMemoryEventStore();
        var eventStore = new CountingEventStore(innerStore);
        var views = new CountingViewStore(new InMemoryViewStore());
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions(); // library defaults (1000 / 200 ms)
        var cache = new ProjectionReadCache(options);
        var meta = new EventMetadata { Timestamp = new DateTime(2026, 8, 18, 0, 0, 0, DateTimeKind.Utc), UserId = "delivery" };

        await AppendInOrderAsync(eventStore, spec, meta);
        var head = await eventStore.GetCurrentSequenceAsync();
        Assert.Equal(spec.Head, head);

        var regs = DeliveryRegs();
        var manager = CreateManager(regs, eventStore, views, checkpoints, options, cache);
        var sw = Stopwatch.StartNew();
        try
        {
            await manager.StartAllAsync();
            await WaitUntilCaughtUpAsync(manager, spec.Head, timeout);
            sw.Stop();

            await WaitForGoldViewsAsync(views, spec, timeout);

            return new DeliveryRunRow(
                Head: spec.Head,
                LastApplied: manager.StorageKeys.Select(k => manager.GetSnapshot(k)!.Value).ToArray(),
                Enumerated: eventStore.EventsEnumerated,
                HandleInvocations: DeliveryHandleCounters.HandleTotal,
                ProcessEventInvocations: Interlocked.Read(ref DeliveryHandleCounters.ProcessEvent),
                HandleGlobal: Interlocked.Read(ref DeliveryHandleCounters.Global),
                HandleDcb: Interlocked.Read(ref DeliveryHandleCounters.Dcb),
                HandleSingleStream: Interlocked.Read(ref DeliveryHandleCounters.SingleStream),
                FlushWaves: views.FlushWaves,
                FlushGlobal: views.FlushWavesFor("DeliveryGlobalCount:v1"),
                FlushDcb: views.FlushWavesFor("DeliveryDcbCount:v1"),
                FlushSingleStream: views.FlushWavesFor("DeliveryStreamCount:v1"),
                ViewWrites: views.ViewWrites,
                Pms: sw.Elapsed.TotalMilliseconds,
                Spec: spec);
        }
        finally
        {
            await manager.StopAllAsync();
        }
    }

    private async Task<DeliveryRunRow> RunRebuildAsync(int noiseCount, int matchCount, TimeSpan timeout)
    {
        DeliveryHandleCounters.Reset();
        var spec = DeliverySliceSpec.Build(noiseCount, matchCount);
        var innerStore = new InMemoryEventStore();
        var eventStore = new CountingEventStore(innerStore);
        var views = new CountingViewStore(new InMemoryViewStore());
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions();
        var cache = new ProjectionReadCache(options);
        var meta = new EventMetadata { Timestamp = new DateTime(2026, 8, 18, 0, 0, 0, DateTimeKind.Utc), UserId = "delivery" };

        await AppendInOrderAsync(eventStore, spec, meta);

        var regs = DeliveryRegs();
        var manager = CreateManager(regs, eventStore, views, checkpoints, options, cache);
        try
        {
            await manager.StartAllAsync();
            await WaitUntilCaughtUpAsync(manager, spec.Head, timeout);
            await WaitForGoldViewsAsync(views, spec, timeout);
        }
        finally
        {
            await manager.StopAllAsync();
        }

        foreach (var reg in regs)
        {
            await views.DeleteAllViewsAsync(reg.StorageKey);
            await checkpoints.DeleteCheckpointAsync(reg.StorageKey, nodeId: null);
            cache.Clear(reg.StorageKey);
        }

        DeliveryHandleCounters.Reset();
        eventStore.ResetEnumerated();
        views.ResetCounts();
        var rebuild = CreateManager(regs, eventStore, views, checkpoints, options, cache);
        var sw = Stopwatch.StartNew();
        try
        {
            await rebuild.StartAllAsync();
            await WaitUntilCaughtUpAsync(rebuild, spec.Head, timeout);
            sw.Stop();
            await WaitForGoldViewsAsync(views, spec, timeout);

            return new DeliveryRunRow(
                Head: spec.Head,
                LastApplied: rebuild.StorageKeys.Select(k => rebuild.GetSnapshot(k)!.Value).ToArray(),
                Enumerated: eventStore.EventsEnumerated,
                HandleInvocations: DeliveryHandleCounters.HandleTotal,
                ProcessEventInvocations: Interlocked.Read(ref DeliveryHandleCounters.ProcessEvent),
                HandleGlobal: Interlocked.Read(ref DeliveryHandleCounters.Global),
                HandleDcb: Interlocked.Read(ref DeliveryHandleCounters.Dcb),
                HandleSingleStream: Interlocked.Read(ref DeliveryHandleCounters.SingleStream),
                FlushWaves: views.FlushWaves,
                FlushGlobal: views.FlushWavesFor("DeliveryGlobalCount:v1"),
                FlushDcb: views.FlushWavesFor("DeliveryDcbCount:v1"),
                FlushSingleStream: views.FlushWavesFor("DeliveryStreamCount:v1"),
                ViewWrites: views.ViewWrites,
                Pms: sw.Elapsed.TotalMilliseconds,
                Spec: spec);
        }
        finally
        {
            await rebuild.StopAllAsync();
        }
    }

    internal static ProjectionRegistration[] DeliveryRegs() =>
    [
        ProjectionScanner.TryBuildRegistration(typeof(DeliveryGlobalCountProjection))!,
        ProjectionScanner.TryBuildRegistration(typeof(DeliveryDcbCountProjection))!,
        ProjectionScanner.TryBuildRegistration(typeof(DeliveryStreamCountProjection))!
    ];

    internal static BoundedContextProjectionRunnerManager CreateManager(
        IReadOnlyList<ProjectionRegistration> regs,
        IEventStore eventStore,
        IViewStore views,
        ICheckpointStore checkpoints,
        LightweightProjectionOptions options,
        IProjectionReadCache? cache = null) =>
        new(
            regs,
            reg => new LightweightProjectionRunnerService(
                reg, eventStore, views, checkpoints,
                new SingleNodePartitioningService(), options, readCache: cache),
            logger: null,
            eventStore,
            options);

    private static async Task AppendInOrderAsync(
        CountingEventStore store,
        DeliverySliceSpec spec,
        EventMetadata meta)
    {
        const int maxBatch = 1000;
        var batch = new List<IEvent>(maxBatch);
        string? stream = null;

        async Task FlushAsync()
        {
            if (batch.Count == 0 || stream is null)
                return;
            await store.AppendAsync(stream, batch, metadata: meta);
            batch.Clear();
        }

        foreach (var (streamId, evt) in spec.Events)
        {
            if (stream is not null && (stream != streamId || batch.Count >= maxBatch))
                await FlushAsync();
            stream = streamId;
            batch.Add(evt);
        }

        await FlushAsync();
    }

    internal static async Task WaitUntilCaughtUpAsync(
        IProjectionRunnerManager manager,
        long head,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!cts.IsCancellationRequested)
        {
            if (manager.StorageKeys.All(k => (manager.GetSnapshot(k)?.LastAppliedSequence ?? -1) >= head))
                return;

            try
            {
                await Task.Delay(10, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        var detail = string.Join(
            ", ",
            manager.StorageKeys.Select(k =>
            {
                var snap = manager.GetSnapshot(k);
                return $"{k}={snap?.LastAppliedSequence}";
            }));
        throw new TimeoutException(
            $"Runners did not reach lastApplied >= {head} within {timeout}. Last: {detail}.");
    }

    private static async Task WaitForGoldViewsAsync(
        IViewStore views,
        DeliverySliceSpec spec,
        TimeSpan timeout)
    {
        var globalJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "DeliveryGlobalCount:v1",
            ProjectionInstanceIds.UnpartitionedBase,
            timeout,
            json => JsonSerializer.Deserialize<DeliveryCountView>(json, ProjectionViewJson.Read)?.Count == spec.GlobalMatches);
        Assert.NotNull(globalJson);

        var dcbJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "DeliveryDcbCount:v1",
            ProjectionInstanceIds.UnpartitionedBase,
            timeout,
            json => JsonSerializer.Deserialize<DeliveryCountView>(json, ProjectionViewJson.Read)?.Count == spec.CounterMatches);
        Assert.NotNull(dcbJson);

        foreach (var (instanceId, expected) in spec.SingleStreamGold)
        {
            var streamJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
                views,
                "DeliveryStreamCount:v1",
                instanceId,
                timeout,
                json => JsonSerializer.Deserialize<DeliveryCountView>(json, ProjectionViewJson.Read)?.Count == expected);
            Assert.NotNull(streamJson);
        }
    }

    private static void AssertGold(DeliveryRunRow row)
    {
        Assert.Equal(row.Spec.GlobalMatches, row.HandleGlobal);
        Assert.Equal(row.Spec.CounterMatches, row.HandleDcb);
        Assert.Equal(row.Spec.CounterMatches, row.HandleSingleStream);
        Assert.Equal(row.Spec.GlobalMatches + (2L * row.Spec.CounterMatches), row.HandleInvocations);
        Assert.Equal(0, row.ProcessEventInvocations);
    }

    private void WriteResultsRow(string label, DeliveryRunRow row)
    {
        var lastApplied = string.Join(
            "; ",
            row.LastApplied.Select(s => $"{s.StorageKey}={s.LastAppliedSequence}"));
        _output.WriteLine(
            "{0} | H={1} | lastApplied=[{2}] | enumerated={3} | Handle={4} (G={5} DCB={6} SS={7}) | ProcessEvent={8} | flushes={9} (G={10} DCB={11} SS={12}) writes={13} | P={14:F0} ms | W=n/a (in-memory sequential)",
            label,
            row.Head,
            lastApplied,
            row.Enumerated,
            row.HandleInvocations,
            row.HandleGlobal,
            row.HandleDcb,
            row.HandleSingleStream,
            row.ProcessEventInvocations,
            row.FlushWaves,
            row.FlushGlobal,
            row.FlushDcb,
            row.FlushSingleStream,
            row.ViewWrites,
            row.Pms);
    }

    private readonly record struct DeliveryRunRow(
        long Head,
        ProjectionRunnerObservabilitySnapshot[] LastApplied,
        long Enumerated,
        long HandleInvocations,
        long ProcessEventInvocations,
        long HandleGlobal,
        long HandleDcb,
        long HandleSingleStream,
        long FlushWaves,
        long FlushGlobal,
        long FlushDcb,
        long FlushSingleStream,
        long ViewWrites,
        double Pms,
        DeliverySliceSpec Spec);
}
