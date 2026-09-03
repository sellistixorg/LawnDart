namespace LawnDart.Projections.Checkpoints;

/// <summary>
/// Interface for storing and retrieving projection checkpoints.
/// </summary>
public interface ICheckpointStore
{
    /// <summary>
    /// Gets the checkpoint for a projection type on a specific node.
    /// </summary>
    /// <param name="projectionType">The projection type name.</param>
    /// <param name="nodeId">The node instance identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The checkpoint, or null if none exists.</returns>
    Task<ProjectionCheckpoint?> GetCheckpointAsync(
        string projectionType,
        int nodeId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Saves a checkpoint for a projection type on a specific node.
    /// </summary>
    /// <param name="checkpoint">The checkpoint to save.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SaveCheckpointAsync(
        ProjectionCheckpoint checkpoint,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets all checkpoints for a projection type across all nodes.
    /// </summary>
    /// <param name="projectionType">The projection type name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Collection of checkpoints for the projection type.</returns>
    Task<IEnumerable<ProjectionCheckpoint>> GetCheckpointsForProjectionAsync(
        string projectionType,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets all checkpoints across all projection types and nodes.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Collection of all checkpoints.</returns>
    Task<IEnumerable<ProjectionCheckpoint>> GetAllCheckpointsAsync(
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets the checkpoint for a specific stream instance.
    /// Used by per-stream projections for seamless scaling.
    /// </summary>
    /// <param name="projectionType">The projection type name.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The checkpoint, or null if none exists.</returns>
    Task<long?> GetStreamCheckpointAsync(
        string projectionType,
        string streamId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Saves a checkpoint for a specific stream instance.
    /// Used by per-stream projections for seamless scaling.
    /// </summary>
    /// <param name="projectionType">The projection type name.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="sequencePosition">The sequence position checkpoint.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SaveStreamCheckpointAsync(
        string projectionType,
        string streamId,
        long sequencePosition,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes checkpoints for a projection type.
    /// When <paramref name="nodeId"/> is <c>null</c> (the default), all node checkpoints for the
    /// projection type are deleted — this is the correct default for a full rebuild so that a
    /// future multi-node deployment never partially resets one partition only.
    /// When <paramref name="nodeId"/> is specified, only that node's checkpoint is removed.
    /// </summary>
    /// <param name="projectionType">The projection type (StorageKey).</param>
    /// <param name="nodeId">The node identifier to delete, or <c>null</c> to delete all nodes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task DeleteCheckpointAsync(
        string projectionType,
        int? nodeId = null,
        CancellationToken cancellationToken = default);
}
