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
    /// Order:
    /// <list type="number">
    ///   <item>Stop the runner, so an in-flight flush cannot write the old state back.</item>
    ///   <item>Clear the checkpoint, then delete the views. When both stores are the same object and it implements <see cref="IProjectionReset"/>, both happen in one transaction, checkpoint first.</item>
    ///   <item>Start a new runner. No checkpoint means it starts empty and does not load stored views.</item>
    /// </list>
    /// </para>
    /// <para>
    /// If <see cref="IProjectionRunnerManager.StartAsync"/> fails after the clear, the stores
    /// have no checkpoint and a retry of <see cref="RebuildAsync"/> is safe.
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
                "For multi-node deployments use DeleteCheckpointAsync, then DeleteAllViewsAsync, " +
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

        // Clear the checkpoint first. A waiting copy that still has the old epoch
        // cannot save once that reset is visible, and then the views go.
        if (ReferenceEquals(_checkpointStore, _viewStore) && _checkpointStore is IProjectionReset reset)
        {
            await reset.ClearCheckpointAndViewsAsync(storageKey, ct).ConfigureAwait(false);
            _logger?.LogDebug("RebuildAsync: checkpoint cleared and views deleted for '{StorageKey}'", storageKey);
        }
        else
        {
            await _checkpointStore.DeleteCheckpointAsync(storageKey, nodeId: null, ct).ConfigureAwait(false);
            _logger?.LogDebug("RebuildAsync: checkpoints cleared for '{StorageKey}'", storageKey);
            await _viewStore.DeleteAllViewsAsync(storageKey, ct).ConfigureAwait(false);
            _logger?.LogDebug("RebuildAsync: views deleted for '{StorageKey}'", storageKey);
        }

        // 3. Restart cold from sequence 0.
        await _runnerManager.StartAsync(storageKey, ct).ConfigureAwait(false);

        _logger?.LogInformation(
            "RebuildAsync: rebuild of '{StorageKey}' complete", storageKey);
    }

    /// <inheritdoc />
    public async Task<ProjectionCheckpoint?> GetCheckpointAsync(
        string storageKey,
        int nodeId,
        CancellationToken ct = default)
    {
        var checkpoint = await _checkpointStore.GetCheckpointAsync(storageKey, nodeId, ct).ConfigureAwait(false);
        if (checkpoint is null || checkpoint.LastSequencePosition < 0)
            return null;
        return checkpoint;
    }

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
