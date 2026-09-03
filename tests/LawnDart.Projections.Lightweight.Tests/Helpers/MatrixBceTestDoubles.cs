using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Helpers;

/// <summary>Delays <see cref="SaveViewsAsync"/> so concurrent GETs can race the flush (Matrix B8).</summary>
internal sealed class DelayedSaveViewsStore : IViewStore
{
    private readonly IViewStore _inner;
    private readonly TimeSpan _delay;
    private int _saveViewsCalls;
    private readonly TaskCompletionSource _entered =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public DelayedSaveViewsStore(IViewStore inner, TimeSpan delay)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _delay = delay;
    }

    public int SaveViewsCalls => Volatile.Read(ref _saveViewsCalls);

    /// <summary>Completes when the first <see cref="SaveViewsAsync"/> has started delaying.</summary>
    public Task FlushEntered => _entered.Task;

    public Task SaveViewAsync(
        string projectionType, string instanceId, string viewData, long checkpoint,
        CancellationToken cancellationToken = default)
        => _inner.SaveViewAsync(projectionType, instanceId, viewData, checkpoint, cancellationToken);

    public async Task SaveViewsAsync(
        string projectionType,
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
        CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _saveViewsCalls);
        _entered.TrySetResult();
        await Task.Delay(_delay, cancellationToken);
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
