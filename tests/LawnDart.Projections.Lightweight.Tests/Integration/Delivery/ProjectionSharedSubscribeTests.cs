using System.Diagnostics;
using System.Text.Json;
using LawnDart;
using Xunit.Abstractions;
using LawnDart.EventSourcing.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration.Delivery;

/// <summary>Shared pipe isolation, attach, and independent checkpoints.</summary>
[Collection(ProjectionDeliveryCounterCollection.Name)]
[Trait("Category", "Delivery")]
[Trait("Category", "Integration")]
public sealed class ProjectionSharedSubscribeTests
{
    private readonly ITestOutputHelper _output;

    public ProjectionSharedSubscribeTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task Rebuild_one_at_zero_does_not_rewind_live_pipe()
    {
        var spec = DeliverySliceSpec.Build(ProjectionDeliveryFixture.CiNoiseCount, ProjectionDeliveryFixture.MatchCount);
        var inner = new InMemoryEventStore();
        var store = new CountingEventStore(inner);
        var views = new CountingViewStore(new InMemoryViewStore());
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions { PollInterval = TimeSpan.FromMilliseconds(10) };
        var cache = new ProjectionReadCache(options);
        var meta = new EventMetadata { Timestamp = new DateTime(2026, 8, 18, 0, 0, 0, DateTimeKind.Utc), UserId = "step5" };

        await AppendDatasetAsync(store, spec, meta);
        var regs = ProjectionDeliveryFixture.DeliveryRegs();
        var manager = ProjectionDeliveryFixture.CreateManager(regs, store, views, checkpoints, options, cache);

        await manager.StartAllAsync();
        await ProjectionDeliveryFixture.WaitUntilCaughtUpAsync(manager, spec.Head, TimeSpan.FromSeconds(30));
        await WaitGoldAsync(views, spec, TimeSpan.FromSeconds(20));

        var liveBefore = manager.StorageKeys.ToDictionary(
            k => k,
            k => manager.GetSnapshot(k)!.Value.LastAppliedSequence);
        Assert.All(liveBefore.Values, v => Assert.True(v >= spec.Head));

        const string rebuiltKey = "DeliveryGlobalCount:v1";
        await manager.StopAsync(rebuiltKey);
        await views.DeleteAllViewsAsync(rebuiltKey);
        await checkpoints.DeleteCheckpointAsync(rebuiltKey, nodeId: null);
        cache.Clear(rebuiltKey);
        store.ResetEnumerated();
        views.ResetCounts();
        DeliveryHandleCounters.Reset();

        var liveSw = Stopwatch.StartNew();
        await manager.StartAsync(rebuiltKey);
        await ProjectionDeliveryFixture.WaitUntilCaughtUpAsync(manager, spec.Head, TimeSpan.FromSeconds(30));
        liveSw.Stop();
        await WaitGoldAsync(views, spec, TimeSpan.FromSeconds(20));

        foreach (var key in liveBefore.Keys.Where(k => k != rebuiltKey))
        {
            var after = manager.GetSnapshot(key)!.Value.LastAppliedSequence;
            Assert.True(
                after >= liveBefore[key],
                $"Live {key} rewound from {liveBefore[key]} to {after} during private rebuild.");
        }

        Assert.True(
            store.EventsEnumerated < spec.Head,
            $"Private rebuild must not rescan the full log × runners ({store.EventsEnumerated} vs H={spec.Head}).");
        Assert.Equal(spec.GlobalMatches, Interlocked.Read(ref DeliveryHandleCounters.Global));
        _output.WriteLine(
            "Rebuild isolation | enumerated={0} | rebuild P={1:F0} ms | live lastApplied stayed at H={2}",
            store.EventsEnumerated, liveSw.Elapsed.TotalMilliseconds, spec.Head);

        await manager.StopAllAsync();
    }

    [Fact]
    public async Task Shared_pipe_checkpoints_are_independent_per_projection()
    {
        var spec = DeliverySliceSpec.Build(2_000, 60);
        var inner = new InMemoryEventStore();
        var store = new CountingEventStore(inner);
        var views = new CountingViewStore(new InMemoryViewStore());
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions { CheckpointInterval = 10 };
        var cache = new ProjectionReadCache(options);
        var meta = new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "step5" };

        await AppendDatasetAsync(store, spec, meta);
        var regs = ProjectionDeliveryFixture.DeliveryRegs();
        var manager = ProjectionDeliveryFixture.CreateManager(regs, store, views, checkpoints, options, cache);

        await manager.StartAllAsync();
        await ProjectionDeliveryFixture.WaitUntilCaughtUpAsync(manager, spec.Head, TimeSpan.FromSeconds(20));
        await manager.StopAllAsync();

        var keys = regs.Select(r => r.StorageKey).ToArray();
        var ckpts = new List<long>();
        foreach (var key in keys)
        {
            var cp = await checkpoints.GetCheckpointAsync(key, nodeId: 0);
            Assert.NotNull(cp);
            ckpts.Add(cp!.LastSequencePosition);
        }

        Assert.All(ckpts, p => Assert.True(p >= spec.Head - 1, $"checkpoint {p} behind H={spec.Head}"));
        Assert.Equal(3, keys.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task Attach_after_private_catch_up_does_not_double_apply()
    {
        var spec = DeliverySliceSpec.Build(ProjectionDeliveryFixture.CiNoiseCount, ProjectionDeliveryFixture.MatchCount);
        var inner = new InMemoryEventStore();
        var store = new CountingEventStore(inner);
        var views = new CountingViewStore(new InMemoryViewStore());
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions();
        var cache = new ProjectionReadCache(options);
        var meta = new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "step5" };

        await AppendDatasetAsync(store, spec, meta);
        var regs = ProjectionDeliveryFixture.DeliveryRegs();
        var manager = ProjectionDeliveryFixture.CreateManager(regs, store, views, checkpoints, options, cache);

        await manager.StartAllAsync();
        await ProjectionDeliveryFixture.WaitUntilCaughtUpAsync(manager, spec.Head, TimeSpan.FromSeconds(20));
        await WaitGoldAsync(views, spec, TimeSpan.FromSeconds(15));

        const string rebuiltKey = "DeliveryDcbCount:v1";
        await manager.StopAsync(rebuiltKey);
        await views.DeleteAllViewsAsync(rebuiltKey);
        await checkpoints.DeleteCheckpointAsync(rebuiltKey, nodeId: null);
        cache.Clear(rebuiltKey);

        await manager.StartAsync(rebuiltKey);
        await ProjectionDeliveryFixture.WaitUntilCaughtUpAsync(manager, spec.Head, TimeSpan.FromSeconds(20));
        await WaitGoldAsync(views, spec, TimeSpan.FromSeconds(15));
        await manager.StopAllAsync();
    }

    private static async Task AppendDatasetAsync(
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

    private static async Task WaitGoldAsync(IViewStore views, DeliverySliceSpec spec, TimeSpan timeout)
    {
        var global = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "DeliveryGlobalCount:v1",
            ProjectionInstanceIds.UnpartitionedBase,
            timeout,
            json => JsonSerializer.Deserialize<DeliveryCountView>(json, ProjectionViewJson.Read)?.Count == spec.GlobalMatches);
        Assert.NotNull(global);
        var dcb = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "DeliveryDcbCount:v1",
            ProjectionInstanceIds.UnpartitionedBase,
            timeout,
            json => JsonSerializer.Deserialize<DeliveryCountView>(json, ProjectionViewJson.Read)?.Count == spec.CounterMatches);
        Assert.NotNull(dcb);
    }
}
