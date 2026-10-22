using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Projections.Lightweight;
using LawnDart.Metadata;
using LawnDart.Projections;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public sealed class AtomicFlushRoutingTests
{
    [Fact]
    public async Task SameObject_UsesTheSingleFlush()
    {
        var store = new MemoryProjectionStore();
        var events = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        await events.AppendAsync(
            "tenant1:Counter:same",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
            metadata: Meta());
        var runner = Runner(events, store, store);
        await runner.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntilAsync(() => store.AtomicCalls > 0);
            Assert.Equal(0, store.SeparateViewSaves);
        }
        finally
        {
            await runner.StopAsync(CancellationToken.None);
            runner.Dispose();
        }
    }

    [Fact]
    public async Task ColdStart_DoesNotLoadStoredViews()
    {
        var store = new MemoryProjectionStore();
        const string streamId = "tenant1:Counter:cold";
        await store.SaveViewAsync("CounterSummary:v1", streamId, """{"count":40}""", 9);
        var events = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        await events.AppendAsync(
            streamId,
            [
                new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1),
                new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)
            ],
            metadata: Meta());
        var runner = Runner(events, store, store);
        await runner.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntilAsync(() => CountOf(store, streamId) == 2);
            Assert.Equal(2, CountOf(store, streamId));
        }
        finally
        {
            await runner.StopAsync(CancellationToken.None);
            runner.Dispose();
        }
    }

    [Fact]
    public async Task WarmStart_LoadsTheStoredView()
    {
        var store = new MemoryProjectionStore();
        const string streamId = "tenant1:Counter:warm";
        var events = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        await events.AppendAsync(
            streamId,
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
            metadata: Meta());
        var head = await events.GetCurrentSequenceAsync();
        await store.SaveCheckpointAsync(new ProjectionCheckpoint
        {
            ProjectionType = "CounterSummary:v1",
            NodeId = 0,
            LastSequencePosition = head,
            LastUpdated = DateTime.UtcNow,
            TotalEventsProcessed = 1
        });
        await store.SaveViewAsync("CounterSummary:v1", streamId, """{"count":10}""", head);
        await events.AppendAsync(
            streamId,
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
            metadata: Meta());
        var runner = Runner(events, store, store);
        await runner.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntilAsync(() => CountOf(store, streamId) == 11);
            Assert.Equal(11, CountOf(store, streamId));
        }
        finally
        {
            await runner.StopAsync(CancellationToken.None);
            runner.Dispose();
        }
    }

    [Fact]
    public async Task DifferentObjects_KeepTwoCalls()
    {
        var views = new MemoryProjectionStore();
        var checkpoints = new MemoryProjectionStore();
        var events = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        await events.AppendAsync(
            "tenant1:Counter:split",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
            metadata: Meta());
        var runner = Runner(events, views, checkpoints);
        await runner.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntilAsync(() => checkpoints.SeparateCheckpointSaves > 0);
            Assert.Equal(0, checkpoints.AtomicCalls);
            Assert.Equal(0, views.AtomicCalls);
            Assert.True(views.SeparateViewSaves > 0);
        }
        finally
        {
            await runner.StopAsync(CancellationToken.None);
            runner.Dispose();
        }
    }

    private static LightweightProjectionRunnerService Runner(
        IEventStore events, IViewStore views, ICheckpointStore checkpoints)
        => new(
            new ProjectionRegistration(
                typeof(CounterSummaryProjection),
                typeof(CounterView),
                "CounterSummary",
                ProjectionKind.SingleStream,
                TenantScope.TenantScoped,
                streamType: "Counter"),
            events,
            views,
            checkpoints,
            new SingleNodePartitioningService(),
            new LightweightProjectionOptions
            {
                PollInterval = TimeSpan.FromMilliseconds(20),
                CheckpointInterval = 1,
                BatchSize = 20,
                SkipTailFlushWhileCatchingUp = false,
                UseSubscribe = false
            });

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static int CountOf(MemoryProjectionStore store, string streamId)
    {
        var json = store.GetViewAsync("CounterSummary:v1", streamId).GetAwaiter().GetResult();
        return json is null ? -1 : System.Text.Json.Nodes.JsonNode.Parse(json)!["count"]!.GetValue<int>();
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!condition())
            await Task.Delay(20, cts.Token);
    }

    private sealed class MemoryProjectionStore : IViewStore, ICheckpointStore, IAtomicProjectionFlush
    {
        private readonly Dictionary<string, (string Json, long Checkpoint)> _views = new(StringComparer.Ordinal);
        private ProjectionCheckpoint? _checkpoint;
        public int AtomicCalls;
        public int SeparateViewSaves;
        public int SeparateCheckpointSaves;

        public Task SaveViewsAndCheckpointAsync(
            string projectionType,
            IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
            ProjectionCheckpoint checkpoint,
            CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref AtomicCalls);
            foreach (var view in views)
                _views[view.InstanceId] = (view.ViewData, view.Checkpoint);
            _checkpoint = checkpoint;
            return Task.CompletedTask;
        }

        public Task SaveViewAsync(string projectionType, string instanceId, string viewData, long checkpoint, CancellationToken cancellationToken = default)
            => SaveViewsAsync(projectionType, [(instanceId, viewData, checkpoint)], cancellationToken);

        public Task SaveViewsAsync(
            string projectionType,
            IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
            CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref SeparateViewSaves);
            foreach (var view in views)
                _views[view.InstanceId] = (view.ViewData, view.Checkpoint);
            return Task.CompletedTask;
        }

        public Task<string?> GetViewAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
            => Task.FromResult(_views.TryGetValue(instanceId, out var view) ? view.Json : null);

        public Task<IEnumerable<(string InstanceId, string ViewData)>> GetViewsByTypeAsync(string projectionType, CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<(string, string)>>(_views.Select(pair => (pair.Key, pair.Value.Json)).ToList());

        public Task DeleteViewAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
        {
            _views.Remove(instanceId);
            return Task.CompletedTask;
        }

        public Task<(string ViewData, long Checkpoint)?> GetViewWithCheckpointAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
            => Task.FromResult(_views.TryGetValue(instanceId, out var view) ? (view.Json, view.Checkpoint) : ((string, long)?)null);

        public Task DeleteAllViewsAsync(string projectionType, CancellationToken cancellationToken = default)
        {
            _views.Clear();
            return Task.CompletedTask;
        }

        public Task<ProjectionCheckpoint?> GetCheckpointAsync(string projectionType, int nodeId, CancellationToken cancellationToken = default)
            => Task.FromResult(_checkpoint);

        public Task SaveCheckpointAsync(ProjectionCheckpoint checkpoint, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref SeparateCheckpointSaves);
            _checkpoint = checkpoint;
            return Task.CompletedTask;
        }

        public Task<IEnumerable<ProjectionCheckpoint>> GetCheckpointsForProjectionAsync(string projectionType, CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<ProjectionCheckpoint>>(_checkpoint is null ? [] : [_checkpoint]);

        public Task<IEnumerable<ProjectionCheckpoint>> GetAllCheckpointsAsync(CancellationToken cancellationToken = default)
            => GetCheckpointsForProjectionAsync(string.Empty, cancellationToken);

        public Task<long?> GetStreamCheckpointAsync(string projectionType, string streamId, CancellationToken cancellationToken = default)
            => Task.FromResult(_views.TryGetValue(streamId, out var view) ? view.Checkpoint : (long?)null);

        public Task SaveStreamCheckpointAsync(string projectionType, string streamId, long sequencePosition, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task DeleteCheckpointAsync(string projectionType, int? nodeId = null, CancellationToken cancellationToken = default)
        {
            _checkpoint = null;
            return Task.CompletedTask;
        }
    }
}
