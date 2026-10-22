namespace LawnDart.Projections.Storage;

/// <summary>
/// Clears one projection's checkpoint and views in one transaction.
/// </summary>
/// <remarks>
/// The rebuild path calls this when the checkpoint store and the view store are
/// the same object. The checkpoint is cleared first. A store that does not
/// implement this is cleared with two calls, checkpoint first.
/// </remarks>
public interface IProjectionReset
{
    /// <summary>
    /// Clears every checkpoint for <paramref name="projectionType"/> and deletes its views.
    /// Both happen in one transaction. The checkpoint is cleared first.
    /// </summary>
    /// <param name="projectionType">Projection storage key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ClearCheckpointAndViewsAsync(
        string projectionType,
        CancellationToken cancellationToken = default);
}
