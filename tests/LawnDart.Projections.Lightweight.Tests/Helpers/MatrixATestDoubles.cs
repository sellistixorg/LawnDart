using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Helpers;

/// <summary>Forces <see cref="SaveViewsAsync"/> to loop <see cref="SaveViewAsync"/> (loop fallback path).</summary>
internal sealed class LoopOnlyViewStore : IViewStore
{
    private readonly IViewStore _inner;
    public int SaveViewCalls { get; private set; }
    public int SaveViewsCalls { get; private set; }

    public LoopOnlyViewStore(IViewStore inner) =>
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));

    public Task SaveViewAsync(
        string projectionType, string instanceId, string viewData, long checkpoint,
        CancellationToken cancellationToken = default)
    {
        SaveViewCalls++;
        return _inner.SaveViewAsync(projectionType, instanceId, viewData, checkpoint, cancellationToken);
    }

    public async Task SaveViewsAsync(
        string projectionType,
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
        CancellationToken cancellationToken = default)
    {
        SaveViewsCalls++;
        ArgumentNullException.ThrowIfNull(views);
        foreach (var (instanceId, viewData, checkpoint) in views)
            await SaveViewAsync(projectionType, instanceId, viewData, checkpoint, cancellationToken);
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

/// <summary>Fails the next N <see cref="SaveViewsAsync"/> calls, then succeeds.</summary>
internal sealed class FailNSaveViewsStore : IViewStore
{
    private readonly IViewStore _inner;
    private int _remainingFailures;

    public FailNSaveViewsStore(IViewStore inner, int failCount)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _remainingFailures = failCount;
    }

    public int SaveViewsAttempts { get; private set; }

    public Task SaveViewAsync(
        string projectionType, string instanceId, string viewData, long checkpoint,
        CancellationToken cancellationToken = default)
        => _inner.SaveViewAsync(projectionType, instanceId, viewData, checkpoint, cancellationToken);

    public async Task SaveViewsAsync(
        string projectionType,
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
        CancellationToken cancellationToken = default)
    {
        SaveViewsAttempts++;
        if (Interlocked.Decrement(ref _remainingFailures) >= 0)
            throw new InvalidOperationException($"Injected SaveViewsAsync failure #{SaveViewsAttempts}");

        await _inner.SaveViewsAsync(projectionType, views, cancellationToken);
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

/// <summary>Fails the next N checkpoint saves (views may already be durable — A5).</summary>
internal sealed class FailNCheckpointStore : ICheckpointStore
{
    private readonly ICheckpointStore _inner;
    private int _remainingFailures;

    public FailNCheckpointStore(ICheckpointStore inner, int failCount)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _remainingFailures = failCount;
    }

    public int SaveAttempts { get; private set; }

    public Task<ProjectionCheckpoint?> GetCheckpointAsync(
        string projectionType, int nodeId, CancellationToken cancellationToken = default)
        => _inner.GetCheckpointAsync(projectionType, nodeId, cancellationToken);

    public async Task SaveCheckpointAsync(ProjectionCheckpoint checkpoint, CancellationToken cancellationToken = default)
    {
        SaveAttempts++;
        if (Interlocked.Decrement(ref _remainingFailures) >= 0)
            throw new InvalidOperationException($"Injected checkpoint failure #{SaveAttempts}");

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
