namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// Lets one running copy do a projection's work for one node number.
/// </summary>
/// <remarks>
/// The runner calls this when a lock is registered. With no lock, every copy runs,
/// which is the previous behavior. The lock does not change how streams are split
/// across node numbers, and it does not change the per-node global view id
/// (<c>global:n{NodeInstance}</c>). It only stops two copies that were both given
/// the same node number from doing that node's work.
/// </remarks>
public interface IProjectionRunnerLock
{
    /// <summary>
    /// Waits until this copy may run <paramref name="projectionType"/> on
    /// <paramref name="nodeId"/>. A second copy with the same node number waits.
    /// </summary>
    /// <param name="projectionType">Projection storage key.</param>
    /// <param name="nodeId">Node number from <see cref="LightweightProjectionOptions.NodeInstance"/>.</param>
    /// <param name="cancellationToken">Cancellation token. Cancelling stops the wait.</param>
    /// <returns>A lease this copy holds until it stops or the lease is lost.</returns>
    Task<IProjectionRunnerLease> AcquireAsync(
        string projectionType,
        int nodeId,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// The right to run one projection on one node number.
/// </summary>
public interface IProjectionRunnerLease : IAsyncDisposable
{
    /// <summary>
    /// True while this copy may save views and the checkpoint.
    /// False after the lock is lost. The runner stops before its next save.
    /// </summary>
    bool IsHeld { get; }

    /// <summary>
    /// The number stored on the checkpoint row when this copy took the lead.
    /// A later copy stores a higher number. A checkpoint save is kept only when
    /// it carries this number.
    /// </summary>
    long Epoch { get; }

    /// <summary>
    /// Database process id of the session that holds the lock.
    /// Null when this copy renews a lease instead of holding a session lock.
    /// </summary>
    int? SessionProcessId { get; }
}
