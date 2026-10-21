using LawnDart.Projections;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Admin;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public sealed class RebuildResetOrderTests
{
    [Fact]
    public async Task SameStore_ClearsCheckpointAndViewsTogether()
    {
        var store = new RecordingStore();
        var admin = Admin(store, store);
        await admin.RebuildAsync("CounterSummary");
        Assert.Equal(1, store.ClearCalls);
        Assert.Equal(0, store.DeleteCheckpointCalls);
        Assert.Equal(0, store.DeleteViewCalls);
    }

    [Fact]
    public async Task DifferentStores_ClearTheCheckpointBeforeTheViews()
    {
        var views = new RecordingStore();
        var checkpoints = new RecordingStore();
        var order = new List<string>();
        views.OnDeleteViews = () => order.Add("views");
        checkpoints.OnDeleteCheckpoint = () => order.Add("checkpoint");
        var admin = Admin(checkpoints, views);
        await admin.RebuildAsync("CounterSummary");
        Assert.Equal(["checkpoint", "views"], order);
        Assert.Equal(0, views.ClearCalls);
        Assert.Equal(0, checkpoints.ClearCalls);
    }

    private static BoundedContextProjectionAdmin Admin(ICheckpointStore checkpoints, IViewStore views)
    {
        var registration = new ProjectionRegistration(
            typeof(CounterSummaryProjection),
            typeof(CounterView),
            "CounterSummary",
            ProjectionKind.SingleStream,
            TenantScope.TenantScoped,
            streamType: "Counter");
        return new BoundedContextProjectionAdmin(
            new IdleManager(),
            checkpoints,
            views,
            new ProjectionRegistrationCatalog([registration]),
            new SingleNodePartitioningService());
    }

    private sealed class IdleManager : IProjectionRunnerManager
    {
        public IReadOnlyCollection<string> StorageKeys { get; } = [];

        public Task StopAsync(string storageKey, CancellationToken ct = default) => Task.CompletedTask;

        public Task StartAsync(string storageKey, CancellationToken ct = default) => Task.CompletedTask;

        public bool IsRunning(string storageKey) => false;

        public ProjectionRunnerObservabilitySnapshot? GetSnapshot(string storageKey) => null;

        public bool TryGetView(string storageKey, string instanceId, out string viewJson, out long sequence)
        {
            viewJson = string.Empty;
            sequence = 0;
            return false;
        }
    }

    private sealed class RecordingStore : IViewStore, ICheckpointStore, IProjectionReset
    {
        public int ClearCalls;
        public int DeleteCheckpointCalls;
        public int DeleteViewCalls;
        public Action? OnDeleteCheckpoint { get; set; }
        public Action? OnDeleteViews { get; set; }

        public Task ClearCheckpointAndViewsAsync(string projectionType, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref ClearCalls);
            return Task.CompletedTask;
        }

        public Task DeleteCheckpointAsync(string projectionType, int? nodeId = null, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref DeleteCheckpointCalls);
            OnDeleteCheckpoint?.Invoke();
            return Task.CompletedTask;
        }

        public Task DeleteAllViewsAsync(string projectionType, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref DeleteViewCalls);
            OnDeleteViews?.Invoke();
            return Task.CompletedTask;
        }

        public Task SaveViewAsync(string projectionType, string instanceId, string viewData, long checkpoint, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<string?> GetViewAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
            => Task.FromResult<string?>(null);

        public Task<IEnumerable<(string InstanceId, string ViewData)>> GetViewsByTypeAsync(string projectionType, CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<(string, string)>>([]);

        public Task DeleteViewAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<(string ViewData, long Checkpoint)?> GetViewWithCheckpointAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
            => Task.FromResult<(string, long)?>(null);

        public Task<ProjectionCheckpoint?> GetCheckpointAsync(string projectionType, int nodeId, CancellationToken cancellationToken = default)
            => Task.FromResult<ProjectionCheckpoint?>(null);

        public Task SaveCheckpointAsync(ProjectionCheckpoint checkpoint, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<IEnumerable<ProjectionCheckpoint>> GetCheckpointsForProjectionAsync(string projectionType, CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<ProjectionCheckpoint>>([]);

        public Task<IEnumerable<ProjectionCheckpoint>> GetAllCheckpointsAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<ProjectionCheckpoint>>([]);

        public Task<long?> GetStreamCheckpointAsync(string projectionType, string streamId, CancellationToken cancellationToken = default)
            => Task.FromResult<long?>(null);

        public Task SaveStreamCheckpointAsync(string projectionType, string streamId, long sequencePosition, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }
}
