using LawnDart.Projections.Checkpoints;

namespace LawnDart.Projections.Lightweight.Admin;

/// <summary>
/// Administrative operations for lightweight projections within a single bounded context.
/// Shape mirrors <c>ISnapshotAdmin</c>.
/// </summary>
/// <remarks>
/// Registered keyed by context name via
/// <c>BoundedContextProjectionExtensions.WithProjections</c>.  Resolve with
/// <c>GetRequiredKeyedService&lt;IProjectionAdmin&gt;("listing")</c>.
/// <para>
/// <strong>v1 constraint:</strong> <see cref="RebuildAsync"/> is only supported in
/// single-node deployments (<c>TotalInstances == 1</c>).  It throws
/// <see cref="NotSupportedException"/> when multiple nodes are configured, because a
/// multi-node rebuild can complete partially without that being detectable.
/// </para>
/// </remarks>
public interface IProjectionAdmin
{
    /// <summary>
    /// Rebuilds a projection from scratch, enforcing this ordering:
    /// Stop runner → Delete all views → Delete all checkpoints (all nodes) → Restart cold.
    /// </summary>
    /// <param name="logicalName">
    /// The logical projection name as registered (e.g. <c>"OrderSummary"</c>).
    /// Resolved via <c>ProjectionRegistrationCatalog</c>; throws
    /// <see cref="InvalidOperationException"/> when the name is unknown.
    /// </param>
    /// <param name="version">
    /// Optional specific version.  When <see langword="null"/> the family's latest version is used.
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <paramref name="logicalName"/> is not registered in this context.
    /// </exception>
    /// <exception cref="NotSupportedException">
    /// Thrown when the context's <c>IPartitioningService.TotalInstances</c> is greater than 1.
    /// </exception>
    Task RebuildAsync(string logicalName, int? version = null, CancellationToken ct = default);

    /// <summary>Returns the stored checkpoint for a specific <paramref name="storageKey"/> and node.</summary>
    Task<ProjectionCheckpoint?> GetCheckpointAsync(
        string storageKey,
        int nodeId,
        CancellationToken ct = default);

    /// <summary>Enumerates the instance IDs of all persisted view instances for a projection type.</summary>
    IAsyncEnumerable<string> EnumerateViewInstancesAsync(
        string storageKey,
        CancellationToken ct = default);

    /// <summary>Lists the storage keys of all projections managed by this context.</summary>
    IReadOnlyCollection<string> ListStorageKeys();
}
