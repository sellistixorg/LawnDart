namespace LawnDart.Projections.Storage;

/// <summary>
/// Interface for storing and retrieving projection views.
/// Implementations may use Redis, SQL, or a hybrid approach.
/// </summary>
public interface IViewStore
{
    /// <summary>
    /// Saves a projection view.
    /// </summary>
    /// <param name="projectionType">The projection type name.</param>
    /// <param name="instanceId">The projection instance identifier.</param>
    /// <param name="viewData">The serialized view data (JSON).</param>
    /// <param name="checkpoint">
    /// Last applied global sequence for this instance (freshness / <c>minSequence</c>).
    /// Not the runner's flush-wave position.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SaveViewAsync(
        string projectionType,
        string instanceId,
        string viewData,
        long checkpoint,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Saves multiple projection views for the same <paramref name="projectionType"/> in one operation.
    /// </summary>
    /// <remarks>
    /// The default implementation loops <see cref="SaveViewAsync"/>. Stores such as
    /// <see cref="SqlViewStore"/> override this for a single round-trip bulk upsert.
    /// Empty <paramref name="views"/> is a no-op.
    /// </remarks>
    async Task SaveViewsAsync(
        string projectionType,
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(views);
        foreach (var (instanceId, viewData, checkpoint) in views)
        {
            await SaveViewAsync(
                projectionType,
                instanceId,
                viewData,
                checkpoint,
                cancellationToken).ConfigureAwait(false);
        }
    }
    
    /// <summary>
    /// Gets a projection view by type and instance ID.
    /// </summary>
    /// <param name="projectionType">The projection type name.</param>
    /// <param name="instanceId">The projection instance identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The view data as JSON, or null if not found.</returns>
    Task<string?> GetViewAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets all views for a specific projection type.
    /// </summary>
    /// <param name="projectionType">The projection type name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Collection of view data (instance ID, JSON).</returns>
    Task<IEnumerable<(string InstanceId, string ViewData)>> GetViewsByTypeAsync(
        string projectionType,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes a projection view.
    /// </summary>
    /// <param name="projectionType">The projection type name.</param>
    /// <param name="instanceId">The projection instance identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task DeleteViewAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets a projection view along with its checkpoint.
    /// Used by per-stream projections to load existing state and resume from checkpoint.
    /// </summary>
    /// <param name="projectionType">The projection type name.</param>
    /// <param name="instanceId">The projection instance identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Tuple of (ViewData, Checkpoint) if found, or null if not found.</returns>
    Task<(string ViewData, long Checkpoint)?> GetViewWithCheckpointAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes all views for a given projection type in a single bulk operation.
    /// Used by the rebuild orchestrator after stopping a runner and before deleting checkpoints,
    /// ensuring the store is clean before a cold restart replays from sequence 0.
    /// </summary>
    /// <remarks>
    /// For Redis this is an O(keyspace) SCAN-based delete; document this when deploying against
    /// large Redis keyspaces.
    /// </remarks>
    /// <param name="projectionType">The projection type whose views should be deleted.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task DeleteAllViewsAsync(
        string projectionType,
        CancellationToken cancellationToken = default);
}
