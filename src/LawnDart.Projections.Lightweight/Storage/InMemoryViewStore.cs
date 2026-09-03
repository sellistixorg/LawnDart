using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace LawnDart.Projections.Storage;

/// <summary>
/// In-memory view store for testing and development.
/// Uses ConcurrentDictionary for thread-safe storage.
/// Data is lost on application restart - not suitable for production.
/// </summary>
public class InMemoryViewStore : IViewStore
{
    private readonly ConcurrentDictionary<string, ViewEntry> _views = new();
    private readonly ILogger<InMemoryViewStore>? _logger;

    public InMemoryViewStore(ILogger<InMemoryViewStore>? logger = null)
    {
        _logger = logger;
    }

    private string GetKey(string projectionType, string instanceId) 
        => $"{projectionType}:{instanceId}";

    public Task SaveViewAsync(
        string projectionType,
        string instanceId,
        string viewData,
        long checkpoint,
        CancellationToken cancellationToken = default)
    {
        var key = GetKey(projectionType, instanceId);
        var entry = new ViewEntry(viewData, checkpoint, DateTime.UtcNow);
        
        _views.AddOrUpdate(key, entry, (k, v) => entry);
        
        _logger?.LogTrace("In-memory view saved: {Key} (checkpoint: {Checkpoint})", key, checkpoint);
        
        return Task.CompletedTask;
    }

    public Task SaveViewsAsync(
        string projectionType,
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(views);
        foreach (var (instanceId, viewData, checkpoint) in views)
        {
            // Synchronous path — reuse single-save logic without async overhead.
            var key = GetKey(projectionType, instanceId);
            _views[key] = new ViewEntry(viewData, checkpoint, DateTime.UtcNow);
        }

        return Task.CompletedTask;
    }

    public Task<string?> GetViewAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        var key = GetKey(projectionType, instanceId);
        
        if (_views.TryGetValue(key, out var entry))
        {
            return Task.FromResult<string?>(entry.ViewData);
        }
        
        return Task.FromResult<string?>(null);
    }

    public Task<IEnumerable<(string InstanceId, string ViewData)>> GetViewsByTypeAsync(
        string projectionType,
        CancellationToken cancellationToken = default)
    {
        var prefix = $"{projectionType}:";
        var results = new List<(string, string)>();
        
        foreach (var kvp in _views)
        {
            if (kvp.Key.StartsWith(prefix, StringComparison.Ordinal))
            {
                var instanceId = kvp.Key.Substring(prefix.Length);
                results.Add((instanceId, kvp.Value.ViewData));
            }
        }
        
        return Task.FromResult<IEnumerable<(string InstanceId, string ViewData)>>(results);
    }

    public Task DeleteViewAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        var key = GetKey(projectionType, instanceId);
        
        if (_views.TryRemove(key, out _))
        {
            _logger?.LogDebug("Deleted view from in-memory store: {Key}", key);
        }
        
        return Task.CompletedTask;
    }

    public Task<(string ViewData, long Checkpoint)?> GetViewWithCheckpointAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        var key = GetKey(projectionType, instanceId);
        
        if (_views.TryGetValue(key, out var entry))
        {
            _logger?.LogDebug(
                "Retrieved view with checkpoint from in-memory store: {Key} | Checkpoint: {Checkpoint}",
                key, entry.Checkpoint);
            
            return Task.FromResult<(string ViewData, long Checkpoint)?>((entry.ViewData, entry.Checkpoint));
        }
        
        return Task.FromResult<(string ViewData, long Checkpoint)?>(null);
    }

    public Task DeleteAllViewsAsync(
        string projectionType,
        CancellationToken cancellationToken = default)
    {
        var prefix = $"{projectionType}:";
        var keysToRemove = _views.Keys
            .Where(k => k.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
        foreach (var key in keysToRemove)
            _views.TryRemove(key, out _);
        _logger?.LogDebug("Deleted {Count} in-memory view(s) for {ProjectionType}", keysToRemove.Count, projectionType);
        return Task.CompletedTask;
    }

    private record ViewEntry(string ViewData, long Checkpoint, DateTime Updated);
}
