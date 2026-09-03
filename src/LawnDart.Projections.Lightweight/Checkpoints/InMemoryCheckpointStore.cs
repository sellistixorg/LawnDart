using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Checkpoints;

/// <summary>
/// In-memory checkpoint store for testing and development.
/// Uses ConcurrentDictionary for thread-safe storage.
/// Data is lost on application restart - not suitable for production.
/// </summary>
public class InMemoryCheckpointStore : ICheckpointStore
{
    private readonly ConcurrentDictionary<string, ProjectionCheckpoint> _checkpoints = new();
    private readonly ILogger<InMemoryCheckpointStore>? _logger;
    private readonly IViewStore _viewStore;

    public InMemoryCheckpointStore(
        IViewStore viewStore,
        ILogger<InMemoryCheckpointStore>? logger = null)
    {
        _viewStore = viewStore ?? throw new ArgumentNullException(nameof(viewStore));
        _logger = logger;
    }

    private string GetKey(string projectionType, int nodeId) 
        => $"{projectionType}:{nodeId}";

    public Task<ProjectionCheckpoint?> GetCheckpointAsync(
        string projectionType,
        int nodeId,
        CancellationToken cancellationToken = default)
    {
        var key = GetKey(projectionType, nodeId);
        
        if (_checkpoints.TryGetValue(key, out var checkpoint))
        {
            // Return a copy to prevent external modification
            return Task.FromResult<ProjectionCheckpoint?>(new ProjectionCheckpoint
            {
                ProjectionType = checkpoint.ProjectionType,
                NodeId = checkpoint.NodeId,
                LastSequencePosition = checkpoint.LastSequencePosition,
                LastUpdated = checkpoint.LastUpdated,
                TotalEventsProcessed = checkpoint.TotalEventsProcessed
            });
        }
        
        return Task.FromResult<ProjectionCheckpoint?>(null);
    }

    public Task SaveCheckpointAsync(
        ProjectionCheckpoint checkpoint,
        CancellationToken cancellationToken = default)
    {
        var key = GetKey(checkpoint.ProjectionType, checkpoint.NodeId);
        
        // CRITICAL: LastSequencePosition must be monotonically increasing.
        // Only update if new value is higher than existing value.
        _checkpoints.AddOrUpdate(key, 
            // Add new checkpoint
            checkpoint,
            // Update existing checkpoint (ensuring monotonic increase)
            (k, existing) =>
            {
                var updated = new ProjectionCheckpoint
                {
                    ProjectionType = checkpoint.ProjectionType,
                    NodeId = checkpoint.NodeId,
                    // Only update LastSequencePosition if new value is higher
                    LastSequencePosition = checkpoint.LastSequencePosition > existing.LastSequencePosition 
                        ? checkpoint.LastSequencePosition 
                        : existing.LastSequencePosition,
                    LastUpdated = checkpoint.LastUpdated,
                    // TotalEventsProcessed is the actual count from the coordinator (single source of truth)
                    TotalEventsProcessed = checkpoint.TotalEventsProcessed
                };
                
                _logger?.LogDebug("Saved checkpoint for {ProjectionType} on Node {NodeId} at position {Position} (was {PreviousPosition})", 
                    checkpoint.ProjectionType, checkpoint.NodeId, updated.LastSequencePosition, existing.LastSequencePosition);
                
                return updated;
            });
        
        _logger?.LogTrace("Saved checkpoint for {ProjectionType} on Node {NodeId} at position {Position}", 
            checkpoint.ProjectionType, checkpoint.NodeId, checkpoint.LastSequencePosition);
        
        return Task.CompletedTask;
    }

    public Task<IEnumerable<ProjectionCheckpoint>> GetCheckpointsForProjectionAsync(
        string projectionType,
        CancellationToken cancellationToken = default)
    {
        var prefix = $"{projectionType}:";
        var results = new List<ProjectionCheckpoint>();
        
        foreach (var kvp in _checkpoints)
        {
            if (kvp.Key.StartsWith(prefix, StringComparison.Ordinal))
            {
                // Return a copy to prevent external modification
                results.Add(new ProjectionCheckpoint
                {
                    ProjectionType = kvp.Value.ProjectionType,
                    NodeId = kvp.Value.NodeId,
                    LastSequencePosition = kvp.Value.LastSequencePosition,
                    LastUpdated = kvp.Value.LastUpdated,
                    TotalEventsProcessed = kvp.Value.TotalEventsProcessed
                });
            }
        }
        
        return Task.FromResult<IEnumerable<ProjectionCheckpoint>>(results.OrderBy(c => c.NodeId));
    }

    public Task<IEnumerable<ProjectionCheckpoint>> GetAllCheckpointsAsync(
        CancellationToken cancellationToken = default)
    {
        var results = _checkpoints.Values.Select(c => new ProjectionCheckpoint
        {
            ProjectionType = c.ProjectionType,
            NodeId = c.NodeId,
            LastSequencePosition = c.LastSequencePosition,
            LastUpdated = c.LastUpdated,
            TotalEventsProcessed = c.TotalEventsProcessed
        }).ToList();
        
        return Task.FromResult<IEnumerable<ProjectionCheckpoint>>(
            results.OrderBy(c => c.ProjectionType).ThenBy(c => c.NodeId));
    }

    public async Task<long?> GetStreamCheckpointAsync(
        string projectionType,
        string streamId,
        CancellationToken cancellationToken = default)
    {
        // Delegate to view store to read checkpoint from view metadata
        var result = await _viewStore.GetViewWithCheckpointAsync(
            projectionType, streamId, cancellationToken);
        
        return result?.Checkpoint;
    }

    public Task SaveStreamCheckpointAsync(
        string projectionType,
        string streamId,
        long sequencePosition,
        CancellationToken cancellationToken = default)
    {
        // Checkpoint is saved with view in ProjectionInstanceActor.SaveView()
        // This method is a no-op - checkpoint already persisted with view
        return Task.CompletedTask;
    }

    public Task DeleteCheckpointAsync(
        string projectionType,
        int? nodeId = null,
        CancellationToken cancellationToken = default)
    {
        if (nodeId.HasValue)
        {
            var key = GetKey(projectionType, nodeId.Value);
            _checkpoints.TryRemove(key, out _);
            _logger?.LogDebug("Deleted checkpoint for {ProjectionType} node {NodeId}", projectionType, nodeId.Value);
        }
        else
        {
            var prefix = $"{projectionType}:";
            var keysToRemove = _checkpoints.Keys
                .Where(k => k.StartsWith(prefix, StringComparison.Ordinal))
                .ToList();
            foreach (var key in keysToRemove)
                _checkpoints.TryRemove(key, out _);
            _logger?.LogDebug("Deleted all checkpoints for {ProjectionType} ({Count} nodes)", projectionType, keysToRemove.Count);
        }
        return Task.CompletedTask;
    }
}
