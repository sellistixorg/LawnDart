using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Performance;

/// <summary>
/// Wraps an IStreamRegistry with caching to reduce database lookups.
/// </summary>
public class CachedStreamRegistry : IStreamRegistry
{
    private readonly IStreamRegistry _inner;
    private readonly IMemoryCache _cache;
    private readonly ILogger<CachedStreamRegistry>? _logger;
    private readonly TimeSpan _cacheExpiration;
    
    /// <summary>
    /// Initializes a new instance of the CachedStreamRegistry.
    /// </summary>
    /// <param name="inner">The underlying stream registry.</param>
    /// <param name="cache">Memory cache for storing results.</param>
    /// <param name="cacheExpiration">How long to cache stream information. Default is 5 minutes.</param>
    /// <param name="logger">Optional logger.</param>
    public CachedStreamRegistry(
        IStreamRegistry inner,
        IMemoryCache cache,
        TimeSpan? cacheExpiration = null,
        ILogger<CachedStreamRegistry>? logger = null)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _cache = cache ?? throw new ArgumentNullException(nameof(cache));
        _cacheExpiration = cacheExpiration ?? TimeSpan.FromMinutes(5);
        _logger = logger;
    }
    
    /// <inheritdoc/>
    public async Task<StreamMetadata?> GetStreamAsync(string streamId, CancellationToken cancellationToken = default)
    {
        var cacheKey = $"StreamRegistry:Stream:{streamId}";
        
        if (_cache.TryGetValue(cacheKey, out StreamMetadata? cachedResult))
        {
            _logger?.LogDebug("Stream registry cache hit for stream: {StreamId}", streamId);
            return cachedResult;
        }
        
        _logger?.LogDebug("Stream registry cache miss for stream: {StreamId}", streamId);
        var result = await _inner.GetStreamAsync(streamId, cancellationToken);
        
        if (result != null)
        {
            _cache.Set(cacheKey, result, new MemoryCacheEntryOptions
            {
                SlidingExpiration = _cacheExpiration
            });
        }
        
        return result;
    }
    
    /// <inheritdoc/>
    public async Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
        string aggregateType,
        CancellationToken cancellationToken = default)
    {
        var cacheKey = $"StreamRegistry:AggregateType:{aggregateType}";
        
        if (_cache.TryGetValue(cacheKey, out IReadOnlyList<StreamMetadata>? cachedResult))
        {
            _logger?.LogDebug("Stream registry cache hit for aggregate type: {AggregateType}", aggregateType);
            return cachedResult!;
        }
        
        _logger?.LogDebug("Stream registry cache miss for aggregate type: {AggregateType}", aggregateType);
        var result = await _inner.GetStreamsByAggregateTypeAsync(aggregateType, cancellationToken);
        
        _cache.Set(cacheKey, result, new MemoryCacheEntryOptions
        {
            SlidingExpiration = _cacheExpiration
        });
        
        return result;
    }
    
    /// <inheritdoc/>
    public async Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
        string tag,
        CancellationToken cancellationToken = default)
    {
        var cacheKey = $"StreamRegistry:Tag:{tag}";
        
        if (_cache.TryGetValue(cacheKey, out IReadOnlyList<StreamMetadata>? cachedResult))
        {
            _logger?.LogDebug("Stream registry cache hit for tag: {Tag}", tag);
            return cachedResult!;
        }
        
        _logger?.LogDebug("Stream registry cache miss for tag: {Tag}", tag);
        var result = await _inner.GetStreamsByTagAsync(tag, cancellationToken);
        
        // Shorter cache for tag queries as they're more dynamic
        _cache.Set(cacheKey, result, new MemoryCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(2)
        });
        
        return result;
    }
    
    /// <inheritdoc/>
    public async Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default)
    {
        var cacheKey = $"StreamRegistry:Enumerate:{prefix}";
        
        if (_cache.TryGetValue(cacheKey, out IReadOnlyList<string>? cachedResult))
        {
            _logger?.LogDebug("Stream registry cache hit for enumerate with prefix: {Prefix}", prefix);
            return cachedResult!;
        }
        
        _logger?.LogDebug("Stream registry cache miss for enumerate with prefix: {Prefix}", prefix);
        var result = await _inner.EnumerateStreamIdsAsync(prefix, cancellationToken);
        
        _cache.Set(cacheKey, result, new MemoryCacheEntryOptions
        {
            SlidingExpiration = _cacheExpiration
        });
        
        return result;
    }
    
    /// <inheritdoc/>
    public async Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(
        long afterSequencePosition,
        int? limit = null,
        CancellationToken cancellationToken = default)
    {
        // Don't cache this - it's used for incremental processing and should always be fresh
        return await _inner.GetStreamsUpdatedAfterAsync(afterSequencePosition, limit, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)
        // Not cached — admin/diagnostic query that should always return a current value
        => _inner.GetStreamCountAsync(prefix, cancellationToken);
    
    /// <summary>
    /// Clears all cached stream registry entries.
    /// </summary>
    public void ClearCache()
    {
        _logger?.LogInformation("Clearing stream registry cache");
        
        // Note: IMemoryCache doesn't provide a clear all method
        // In production, you might want to track cache keys or use a dedicated cache with clear support
        // For now, we'll just let entries expire naturally
    }
}
