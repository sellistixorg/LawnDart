using LawnDart.Projections;

namespace LawnDart.Projections.Storage;

/// <summary>
/// Saves every view in a flush and the checkpoint in one transaction.
/// </summary>
/// <remarks>
/// The runner calls this when the checkpoint store or the view store implements it.
/// A store that does not implement it keeps two calls: views, then the checkpoint.
/// A failure leaves the views and the checkpoint as they were.
/// </remarks>
public interface IAtomicProjectionFlush
{
    /// <summary>
    /// Writes <paramref name="views"/> and <paramref name="checkpoint"/> together.
    /// </summary>
    /// <param name="projectionType">Projection storage key.</param>
    /// <param name="views">
    /// View rows for this flush. Empty writes the checkpoint only.
    /// Duplicate instance ids keep the last row in the list.
    /// </param>
    /// <param name="checkpoint">Checkpoint for this projection and node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SaveViewsAndCheckpointAsync(
        string projectionType,
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
        ProjectionCheckpoint checkpoint,
        CancellationToken cancellationToken = default);
}
