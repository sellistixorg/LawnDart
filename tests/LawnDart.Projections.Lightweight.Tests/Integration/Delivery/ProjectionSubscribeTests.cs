using System.Diagnostics;
using System.Text.Json;
using Xunit.Abstractions;
using LawnDart;
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

/// <summary>Per-runner Subscribe with poll fallback and live type filters.</summary>
[Trait("Category", "Delivery")]
[Trait("Category", "Integration")]
public sealed class ProjectionSubscribeTests
{
    private readonly ITestOutputHelper _output;

    public ProjectionSubscribeTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task Idle_apply_after_catch_up_is_much_faster_than_poll_interval()
    {
        var store = new InMemoryEventStore();
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(200),
            CheckpointInterval = 10,
            BatchSize = 100
        };

        var runner = CreateGlobalRunner(store, views, checkpoints, options);
        await runner.StartAsync(CancellationToken.None);

        await store.AppendAsync("system:Tags:warm", [Tag("warm")], metadata: Meta());
        await WaitCursorAsync(runner, await store.GetCurrentSequenceAsync(), TimeSpan.FromSeconds(10));
        Assert.True(runner.HasLiveSubscription);

        var samples = new List<double>(7);
        for (var i = 0; i < 7; i++)
        {
            var sw = Stopwatch.StartNew();
            await store.AppendAsync($"system:Tags:idle-{i}", [Tag($"idle-{i}")], metadata: Meta());
            var head = await store.GetCurrentSequenceAsync();
            await WaitCursorAsync(runner, head, TimeSpan.FromSeconds(5));
            sw.Stop();
            samples.Add(sw.Elapsed.TotalMilliseconds);
        }

        await runner.StopAsync(CancellationToken.None);

        samples.Sort();
        var median = samples[samples.Count / 2];
        _output.WriteLine(
            "Idle P samples ms=[{0}] median={1:F1}",
            string.Join(", ", samples.Select(s => s.ToString("F1"))),
            median);
        Assert.True(
            median < 50,
            $"Idle apply median {median:F1} ms (samples {string.Join(", ", samples.Select(s => s.ToString("F1")))}) should be < 50 ms.");
    }

    [Fact]
    public async Task Cancel_subscribe_mid_catch_up_poll_recovers_sparse_tail()
    {
        var store = new InMemoryEventStore();
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(10),
            CheckpointInterval = 50,
            BatchSize = 100
        };

        await store.AppendAsync("system:Tags:head", [Tag("head")], metadata: Meta());

        var runner = CreateGlobalRunner(store, views, checkpoints, options);
        await runner.StartAsync(CancellationToken.None);

        await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "GlobalTagIndex:v1",
            "global",
            TimeSpan.FromSeconds(10),
            json => JsonSerializer.Deserialize<GlobalIndexView>(json, ProjectionViewJson.Read)?.TotalTags == 1);
        Assert.True(runner.HasLiveSubscription);
        runner.CancelSubscriptionForTests(allowResubscribe: false);
        Assert.False(runner.HasLiveSubscription);

        var noise = new IEvent[2_500];
        for (var i = 0; i < noise.Length; i++)
            noise[i] = new OrderCreatedLocal(Guid.NewGuid(), DateTime.UtcNow, Guid.NewGuid(), "noise");
        await store.AppendAsync("tenant:OrderAggregate:noise", noise, metadata: Meta());
        await store.AppendAsync("system:Tags:tail", [Tag("tail")], metadata: Meta());

        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "GlobalTagIndex:v1",
            "global",
            TimeSpan.FromSeconds(20),
            json => JsonSerializer.Deserialize<GlobalIndexView>(json, ProjectionViewJson.Read)?.TotalTags == 2);

        await runner.StopAsync(CancellationToken.None);

        Assert.NotNull(viewJson);
        var view = JsonSerializer.Deserialize<GlobalIndexView>(viewJson!, ProjectionViewJson.Read);
        Assert.Equal(2, view!.TotalTags);
    }

    [Fact]
    public async Task Restart_resumes_from_checkpoint_without_skipping_matches()
    {
        var store = new InMemoryEventStore();
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(10),
            CheckpointInterval = 1,
            BatchSize = 50
        };

        await store.AppendAsync("system:Tags:a", [Tag("a"), Tag("b")], metadata: Meta());

        var first = CreateGlobalRunner(store, views, checkpoints, options);
        await first.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "GlobalTagIndex:v1",
            "global",
            TimeSpan.FromSeconds(10),
            json => JsonSerializer.Deserialize<GlobalIndexView>(json, ProjectionViewJson.Read)?.TotalTags == 2);
        await first.StopAsync(CancellationToken.None);

        var checkpoint = await checkpoints.GetCheckpointAsync("GlobalTagIndex:v1", 0);
        Assert.NotNull(checkpoint);
        Assert.True(checkpoint!.LastSequencePosition >= 2);

        await store.AppendAsync("system:Tags:c", [Tag("c")], metadata: Meta());

        var second = CreateGlobalRunner(store, views, checkpoints, options);
        await second.StartAsync(CancellationToken.None);
        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "GlobalTagIndex:v1",
            "global",
            TimeSpan.FromSeconds(10),
            json => JsonSerializer.Deserialize<GlobalIndexView>(json, ProjectionViewJson.Read)?.TotalTags == 3);
        Assert.True(second.HasLiveSubscription);
        await second.StopAsync(CancellationToken.None);

        Assert.NotNull(viewJson);
        var view = JsonSerializer.Deserialize<GlobalIndexView>(viewJson!, ProjectionViewJson.Read);
        Assert.Equal(3, view!.TotalTags);
        Assert.Equal(new[] { "a", "b", "c" }, view.Tags);
    }

    [Fact]
    public async Task Far_behind_idle_subscribe_polls_without_recovery_interval_tax()
    {
        var inner = new InMemoryEventStore();
        var store = new IdleSubscribeEventStore(inner);
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(10),
            CheckpointInterval = 200,
            BatchSize = 500,
            CatchUpFlushThreshold = 1_000,
            SubscribeRecoveryPollInterval = TimeSpan.FromSeconds(3)
        };

        const int n = 5_000;
        var tags = new IEvent[n];
        for (var i = 0; i < n; i++)
            tags[i] = Tag($"t-{i}");
        await inner.AppendAsync("system:Tags:bulk", tags, metadata: Meta());
        var head = await inner.GetCurrentSequenceAsync();

        var runner = CreateGlobalRunner(store, views, checkpoints, options);
        var sw = Stopwatch.StartNew();
        await runner.StartAsync(CancellationToken.None);
        await WaitCursorAsync(runner, head, TimeSpan.FromSeconds(8));
        sw.Stop();
        await runner.StopAsync(CancellationToken.None);

        _output.WriteLine("Far-behind idle Subscribe P={0:F0} ms for H={1}", sw.Elapsed.TotalMilliseconds, head);
        Assert.True(
            sw.Elapsed < TimeSpan.FromSeconds(8),
            $"Poll recover took {sw.Elapsed.TotalMilliseconds:F0} ms; 3s×BatchSize path would exceed 8s.");
    }

    private static LightweightProjectionRunnerService CreateGlobalRunner(
        IEventStore store,
        IViewStore views,
        ICheckpointStore checkpoints,
        LightweightProjectionOptions options) =>
        new(
            ProjectionScanner.TryBuildRegistration(typeof(GlobalTagIndexProjection))!,
            store,
            views,
            checkpoints,
            new SingleNodePartitioningService(),
            options);

    private static EventMetadata Meta() =>
        new() { Timestamp = DateTime.UtcNow, UserId = "step3" };

    private static GlobalTagged Tag(string tag) =>
        new(Guid.NewGuid(), DateTime.UtcNow, tag);

    private static async Task WaitCursorAsync(
        LightweightProjectionRunnerService runner,
        long head,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!cts.IsCancellationRequested)
        {
            if (runner.GetObservabilitySnapshot().LastAppliedSequence >= head)
                return;
            try { await Task.Delay(5, cts.Token); }
            catch (OperationCanceledException) { break; }
        }

        throw new TimeoutException(
            $"Cursor {runner.GetObservabilitySnapshot().LastAppliedSequence} did not reach {head}.");
    }
}
