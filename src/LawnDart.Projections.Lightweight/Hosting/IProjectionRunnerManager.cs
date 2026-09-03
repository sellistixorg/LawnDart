namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// Registry for per-projection-type runner lifecycle within a single bounded context.
/// </summary>
/// <remarks>
/// Registered keyed by context name alongside <c>IViewStore</c> / <c>ICheckpointStore</c> /
/// <c>ProjectionRegistrationCatalog</c> in <c>BoundedContextProjectionExtensions.WithProjections</c>.
/// Use <c>IProjectionAdmin</c> for high-level rebuild operations that enforce the correct
/// stop → delete → restart ordering.
/// </remarks>
public interface IProjectionRunnerManager
{
    /// <summary>Storage keys for all projections managed by this context's runner manager.</summary>
    IReadOnlyCollection<string> StorageKeys { get; }

    /// <summary>
    /// Stops the runner for <paramref name="storageKey"/>. If the runner is already stopped,
    /// this is a no-op.
    /// </summary>
    Task StopAsync(string storageKey, CancellationToken ct = default);

    /// <summary>
    /// Starts a <strong>fresh</strong> <see cref="LightweightProjectionRunnerService"/> instance
    /// for <paramref name="storageKey"/>, discarding any prior in-memory state
    /// (<c>_instances</c>, <c>_dirtyInstances</c>, <c>_lastCheckpointSequence</c>).
    /// If a runner is currently running it is stopped and disposed first.
    /// </summary>
    Task StartAsync(string storageKey, CancellationToken ct = default);

    /// <summary>Returns <see langword="true"/> when the runner for <paramref name="storageKey"/> is active.</summary>
    bool IsRunning(string storageKey);

    /// <summary>
    /// Returns a point-in-time observability snapshot for the runner, or <see langword="null"/>
    /// when the runner is stopped.
    /// </summary>
    ProjectionRunnerObservabilitySnapshot? GetSnapshot(string storageKey);

    /// <summary>
    /// Tries to read a live in-memory view from the running projection's working set.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when this node currently holds <paramref name="instanceId"/> in memory.
    /// </returns>
    bool TryGetView(string storageKey, string instanceId, out string viewJson, out long sequence);
}
