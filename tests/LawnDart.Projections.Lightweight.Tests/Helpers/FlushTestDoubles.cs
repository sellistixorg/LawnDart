using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Helpers;

/// <summary>
/// Blocks each <see cref="IViewStore.SaveViewAsync"/> until the configured concurrent-caller
/// count is reached, proving the runner issues concurrent view writes.
/// </summary>
internal sealed class ConcurrentTrackingViewStore : IViewStore
{
    private readonly IViewStore _inner;
    private readonly int _requiredConcurrent;
    private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _maxInflight;
    private int _inflight;

    public ConcurrentTrackingViewStore(IViewStore inner, int requiredConcurrent = 4)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _requiredConcurrent = requiredConcurrent;
    }

    private int _saveCount;

    public int MaxInflight => Volatile.Read(ref _maxInflight);
    public int SaveCount => Volatile.Read(ref _saveCount);

    public async Task SaveViewAsync(
        string projectionType,
        string instanceId,
        string viewData,
        long checkpoint,
        CancellationToken cancellationToken = default)
    {
        var now = Interlocked.Increment(ref _inflight);
        while (true)
        {
            var snapshot = _maxInflight;
            if (now <= snapshot || Interlocked.CompareExchange(ref _maxInflight, now, snapshot) == snapshot)
                break;
        }

        // Release only when enough callers are in-flight at the same time (true concurrency).
        if (now >= _requiredConcurrent)
            _gate.TrySetResult();

        try
        {
            await _gate.Task.WaitAsync(cancellationToken);
            await _inner.SaveViewAsync(projectionType, instanceId, viewData, checkpoint, cancellationToken);
            Interlocked.Increment(ref _saveCount);
        }
        finally
        {
            Interlocked.Decrement(ref _inflight);
        }
    }

    public Task<string?> GetViewAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
        => _inner.GetViewAsync(projectionType, instanceId, cancellationToken);

    public Task<IEnumerable<(string InstanceId, string ViewData)>> GetViewsByTypeAsync(
        string projectionType, CancellationToken cancellationToken = default)
        => _inner.GetViewsByTypeAsync(projectionType, cancellationToken);

    public Task DeleteViewAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
        => _inner.DeleteViewAsync(projectionType, instanceId, cancellationToken);

    public Task<(string ViewData, long Checkpoint)?> GetViewWithCheckpointAsync(
        string projectionType, string instanceId, CancellationToken cancellationToken = default)
        => _inner.GetViewWithCheckpointAsync(projectionType, instanceId, cancellationToken);

    public Task DeleteAllViewsAsync(string projectionType, CancellationToken cancellationToken = default)
        => _inner.DeleteAllViewsAsync(projectionType, cancellationToken);
}

/// <summary>Fails the first <see cref="FailAfter"/> saves, then succeeds.</summary>
internal sealed class FailThenSucceedViewStore : IViewStore
{
    private readonly IViewStore _inner;
    private int _attempts;

    public FailThenSucceedViewStore(IViewStore inner, int failAfter)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        FailAfter = failAfter;
    }

    public int FailAfter { get; }
    public int Attempts => _attempts;

    public async Task SaveViewAsync(
        string projectionType,
        string instanceId,
        string viewData,
        long checkpoint,
        CancellationToken cancellationToken = default)
    {
        var n = Interlocked.Increment(ref _attempts);
        if (n <= FailAfter)
            throw new InvalidOperationException($"Injected view save failure #{n}");

        await _inner.SaveViewAsync(projectionType, instanceId, viewData, checkpoint, cancellationToken);
    }

    public Task<string?> GetViewAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
        => _inner.GetViewAsync(projectionType, instanceId, cancellationToken);

    public Task<IEnumerable<(string InstanceId, string ViewData)>> GetViewsByTypeAsync(
        string projectionType, CancellationToken cancellationToken = default)
        => _inner.GetViewsByTypeAsync(projectionType, cancellationToken);

    public Task DeleteViewAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
        => _inner.DeleteViewAsync(projectionType, instanceId, cancellationToken);

    public Task<(string ViewData, long Checkpoint)?> GetViewWithCheckpointAsync(
        string projectionType, string instanceId, CancellationToken cancellationToken = default)
        => _inner.GetViewWithCheckpointAsync(projectionType, instanceId, cancellationToken);

    public Task DeleteAllViewsAsync(string projectionType, CancellationToken cancellationToken = default)
        => _inner.DeleteAllViewsAsync(projectionType, cancellationToken);
}

internal sealed class CountingCheckpointStore : ICheckpointStore
{
    private readonly ICheckpointStore _inner;
    private int _saves;

    public CountingCheckpointStore(ICheckpointStore inner) =>
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));

    public int SaveCount => _saves;

    public Task<ProjectionCheckpoint?> GetCheckpointAsync(
        string projectionType, int nodeId, CancellationToken cancellationToken = default)
        => _inner.GetCheckpointAsync(projectionType, nodeId, cancellationToken);

    public async Task SaveCheckpointAsync(ProjectionCheckpoint checkpoint, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _saves);
        await _inner.SaveCheckpointAsync(checkpoint, cancellationToken);
    }

    public Task<IEnumerable<ProjectionCheckpoint>> GetCheckpointsForProjectionAsync(
        string projectionType, CancellationToken cancellationToken = default)
        => _inner.GetCheckpointsForProjectionAsync(projectionType, cancellationToken);

    public Task<IEnumerable<ProjectionCheckpoint>> GetAllCheckpointsAsync(
        CancellationToken cancellationToken = default)
        => _inner.GetAllCheckpointsAsync(cancellationToken);

    public Task<long?> GetStreamCheckpointAsync(
        string projectionType, string streamId, CancellationToken cancellationToken = default)
        => _inner.GetStreamCheckpointAsync(projectionType, streamId, cancellationToken);

    public Task SaveStreamCheckpointAsync(
        string projectionType, string streamId, long sequencePosition,
        CancellationToken cancellationToken = default)
        => _inner.SaveStreamCheckpointAsync(projectionType, streamId, sequencePosition, cancellationToken);

    public Task DeleteCheckpointAsync(
        string projectionType, int? nodeId = null, CancellationToken cancellationToken = default)
        => _inner.DeleteCheckpointAsync(projectionType, nodeId, cancellationToken);
}
