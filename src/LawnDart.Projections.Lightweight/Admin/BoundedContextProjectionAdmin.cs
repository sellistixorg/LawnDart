using Microsoft.Extensions.Logging;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Admin;

/// <summary>
/// Default <see cref="IProjectionAdmin"/> for a single bounded context.
/// </summary>
public sealed class BoundedContextProjectionAdmin : IProjectionAdmin
{
    private readonly IProjectionRunnerManager _runnerManager;
    private readonly ICheckpointStore _checkpointStore;
    private readonly IViewStore _viewStore;
    private readonly ProjectionRegistrationCatalog _catalog;
    private readonly IPartitioningService _partitioning;
    private readonly IProjectionReadCache? _readCache;
    private readonly ILogger<BoundedContextProjectionAdmin>? _logger;

    /// <summary>Creates a new admin instance.</summary>
    public BoundedContextProjectionAdmin(
        IProjectionRunnerManager runnerManager,
        ICheckpointStore checkpointStore,
        IViewStore viewStore,
        ProjectionRegistrationCatalog catalog,
        IPartitioningService partitioning,
        ILogger<BoundedContextProjectionAdmin>? logger = null,
        IProjectionReadCache? readCache = null)
    {
        _runnerManager   = runnerManager   ?? throw new ArgumentNullException(nameof(runnerManager));
        _checkpointStore = checkpointStore ?? throw new ArgumentNullException(nameof(checkpointStore));
        _viewStore       = viewStore       ?? throw new ArgumentNullException(nameof(viewStore));
        _catalog         = catalog         ?? throw new ArgumentNullException(nameof(catalog));
        _partitioning    = partitioning    ?? throw new ArgumentNullException(nameof(partitioning));
        _logger          = logger;
        _readCache       = readCache;
    }

    /// <inheritdoc />
    /// <remarks>
    /// <para>
    /// Ordering (load-bearing):
    /// <list type="number">
    ///   <item>Stop runner — prevents in-flight flushes from resurrecting deleted state.</item>
    ///   <item>Delete views before checkpoints — avoids warm-restore of stale JSON on restart.</item>
    ///   <item>Delete checkpoints for all nodes.</item>
    ///   <item>Start fresh runner — cold-start path handles missing checkpoint as sequence 0.</item>
    /// </list>
    /// </para>
    /// <para>
    /// If <see cref="IProjectionRunnerManager.StartAsync"/> fails after the deletes, stores are
    /// clean and a retry of <see cref="RebuildAsync"/> is safe.
    /// </para>
    /// </remarks>
    public async Task RebuildAsync(
        string logicalName,
        int? version = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalName);

        // Interlock: refuse multi-node deployments to avoid silently partial rebuilds.
        if (_partitioning.TotalInstances > 1)
            throw new NotSupportedException(
                $"RebuildAsync is only supported in single-node deployments (TotalInstances == 1). " +
                $"This context has TotalInstances = {_partitioning.TotalInstances}. " +
                "For multi-node deployments use DeleteAllViewsAsync + DeleteCheckpointAsync " +
                "followed by a rolling restart of all nodes.");

        // Resolve storage key — throws InvalidOperationException for unknown names.
        var reg        = _catalog.Resolve(logicalName, version);
        var storageKey = reg.StorageKey;

        _logger?.LogInformation(
            "RebuildAsync: starting rebuild of '{StorageKey}' ({LogicalName} v{Version})",
            storageKey, logicalName, reg.Version);

        // 1. Stop runner first.
        await _runnerManager.StopAsync(storageKey, ct).ConfigureAwait(false);

        // 1b. Drop hot-cache accelerators so GET cannot serve pre-rebuild bodies.
        _readCache?.Clear(storageKey);

        // 2a. Delete views before checkpoints.
        await _viewStore.DeleteAllViewsAsync(storageKey, ct).ConfigureAwait(false);
        _logger?.LogDebug("RebuildAsync: views deleted for '{StorageKey}'", storageKey);

        // 2b. Delete checkpoints for all nodes (nodeId: null = all nodes).
        await _checkpointStore.DeleteCheckpointAsync(storageKey, nodeId: null, ct).ConfigureAwait(false);
        _logger?.LogDebug("RebuildAsync: checkpoints deleted for '{StorageKey}'", storageKey);

        // 3. Restart cold from sequence 0.
        await _runnerManager.StartAsync(storageKey, ct).ConfigureAwait(false);

        _logger?.LogInformation(
            "RebuildAsync: rebuild of '{StorageKey}' complete", storageKey);
    }

    /// <inheritdoc />
    public Task<ProjectionCheckpoint?> GetCheckpointAsync(
        string storageKey,
        int nodeId,
        CancellationToken ct = default)
        => _checkpointStore.GetCheckpointAsync(storageKey, nodeId, ct);

    /// <inheritdoc />
    public async IAsyncEnumerable<string> EnumerateViewInstancesAsync(
        string storageKey,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var views = await _viewStore.GetViewsByTypeAsync(storageKey, ct).ConfigureAwait(false);
        foreach (var (instanceId, _) in views)
            yield return instanceId;
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> ListStorageKeys() => _runnerManager.StorageKeys;
}
