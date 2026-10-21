using LawnDart.Projections;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Postgres;

/// <summary>
/// One Postgres store for views and checkpoints. The runner saves both in one
/// transaction because this object is both stores. A rebuild clears the checkpoint
/// and deletes the views in one transaction, checkpoint first.
/// </summary>
public sealed class PostgresProjectionStore : IViewStore, ICheckpointStore, IAtomicProjectionFlush, IProjectionReset
{
    private readonly PostgresViewStore _views;
    private readonly PostgresCheckpointStore _checkpoints;

    /// <summary>Creates a store that writes <paramref name="views"/> and <paramref name="checkpoints"/> together.</summary>
    public PostgresProjectionStore(PostgresViewStore views, PostgresCheckpointStore checkpoints)
    {
        ArgumentNullException.ThrowIfNull(views);
        ArgumentNullException.ThrowIfNull(checkpoints);
        _views = views;
        _checkpoints = checkpoints;
    }

    /// <summary>The view store this object forwards to.</summary>
    public PostgresViewStore Views => _views;

    /// <summary>The checkpoint store this object forwards to.</summary>
    public PostgresCheckpointStore Checkpoints => _checkpoints;

    /// <summary>Creates the view table, the checkpoint table, and the flush function.</summary>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        await _views.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        await _checkpoints.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public Task SaveViewAsync(string projectionType, string instanceId, string viewData, long checkpoint, CancellationToken cancellationToken = default)
        => _views.SaveViewAsync(projectionType, instanceId, viewData, checkpoint, cancellationToken);

    /// <inheritdoc/>
    public Task SaveViewsAsync(string projectionType, IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views, CancellationToken cancellationToken = default)
        => _views.SaveViewsAsync(projectionType, views, cancellationToken);

    /// <inheritdoc/>
    public Task<string?> GetViewAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
        => _views.GetViewAsync(projectionType, instanceId, cancellationToken);

    /// <inheritdoc/>
    public Task<IEnumerable<(string InstanceId, string ViewData)>> GetViewsByTypeAsync(string projectionType, CancellationToken cancellationToken = default)
        => _views.GetViewsByTypeAsync(projectionType, cancellationToken);

    /// <inheritdoc/>
    public Task DeleteViewAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
        => _views.DeleteViewAsync(projectionType, instanceId, cancellationToken);

    /// <inheritdoc/>
    public Task<(string ViewData, long Checkpoint)?> GetViewWithCheckpointAsync(string projectionType, string instanceId, CancellationToken cancellationToken = default)
        => _views.GetViewWithCheckpointAsync(projectionType, instanceId, cancellationToken);

    /// <inheritdoc/>
    public Task DeleteAllViewsAsync(string projectionType, CancellationToken cancellationToken = default)
        => _views.DeleteAllViewsAsync(projectionType, cancellationToken);

    /// <inheritdoc/>
    public Task<ProjectionCheckpoint?> GetCheckpointAsync(string projectionType, int nodeId, CancellationToken cancellationToken = default)
        => _checkpoints.GetCheckpointAsync(projectionType, nodeId, cancellationToken);

    /// <inheritdoc/>
    public Task SaveCheckpointAsync(ProjectionCheckpoint checkpoint, CancellationToken cancellationToken = default)
        => _checkpoints.SaveCheckpointAsync(checkpoint, cancellationToken);

    /// <inheritdoc/>
    public Task<IEnumerable<ProjectionCheckpoint>> GetCheckpointsForProjectionAsync(string projectionType, CancellationToken cancellationToken = default)
        => _checkpoints.GetCheckpointsForProjectionAsync(projectionType, cancellationToken);

    /// <inheritdoc/>
    public Task<IEnumerable<ProjectionCheckpoint>> GetAllCheckpointsAsync(CancellationToken cancellationToken = default)
        => _checkpoints.GetAllCheckpointsAsync(cancellationToken);

    /// <inheritdoc/>
    public Task<long?> GetStreamCheckpointAsync(string projectionType, string streamId, CancellationToken cancellationToken = default)
        => _checkpoints.GetStreamCheckpointAsync(projectionType, streamId, cancellationToken);

    /// <inheritdoc/>
    public Task SaveStreamCheckpointAsync(string projectionType, string streamId, long sequencePosition, CancellationToken cancellationToken = default)
        => _checkpoints.SaveStreamCheckpointAsync(projectionType, streamId, sequencePosition, cancellationToken);

    /// <inheritdoc/>
    public Task DeleteCheckpointAsync(string projectionType, int? nodeId = null, CancellationToken cancellationToken = default)
        => _checkpoints.DeleteCheckpointAsync(projectionType, nodeId, cancellationToken);

    /// <inheritdoc/>
    public Task ClearCheckpointAndViewsAsync(string projectionType, CancellationToken cancellationToken = default)
        => _checkpoints.ClearCheckpointAndViewsAsync(projectionType, cancellationToken);

    /// <inheritdoc/>
    public Task SaveViewsAndCheckpointAsync(
        string projectionType,
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
        ProjectionCheckpoint checkpoint,
        CancellationToken cancellationToken = default)
        => _checkpoints.SaveViewsAndCheckpointAsync(projectionType, views, checkpoint, cancellationToken);
}
