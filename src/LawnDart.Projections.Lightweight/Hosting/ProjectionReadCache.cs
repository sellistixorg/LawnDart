using System.Collections.Concurrent;

namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// Thread-safe in-process projection read cache with optional LRU eviction.
/// </summary>
public sealed class ProjectionReadCache : IProjectionReadCache
{
    private readonly ProjectionReadCacheMode _mode;
    private readonly int _maxEntries;
    private readonly int _promoteAfterHits;
    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> _durableHits = new(StringComparer.Ordinal);

    public ProjectionReadCache(LightweightProjectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _mode = options.ReadCacheMode;
        _maxEntries = Math.Max(1, options.ReadCacheMaxEntries);
        _promoteAfterHits = Math.Max(1, options.ReadCachePromoteAfterHits);
    }

    /// <summary>Test/helper constructor.</summary>
    public ProjectionReadCache(
        ProjectionReadCacheMode mode = ProjectionReadCacheMode.Hot,
        int maxEntries = 10_000,
        int promoteAfterHits = 1)
    {
        _mode = mode;
        _maxEntries = Math.Max(1, maxEntries);
        _promoteAfterHits = Math.Max(1, promoteAfterHits);
    }

    /// <inheritdoc />
    public bool TryGet(string storageKey, string instanceId, out ProjectionCachedView view)
    {
        view = default;
        if (_mode == ProjectionReadCacheMode.Off)
            return false;

        var key = MakeKey(storageKey, instanceId);
        if (!_entries.TryGetValue(key, out var entry))
            return false;

        entry.Touch();
        view = new ProjectionCachedView(entry.ViewJson, entry.Sequence);
        return true;
    }

    /// <inheritdoc />
    public void Set(string storageKey, string instanceId, string viewJson, long sequence)
    {
        if (_mode == ProjectionReadCacheMode.Off)
            return;

        ArgumentException.ThrowIfNullOrWhiteSpace(storageKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        ArgumentNullException.ThrowIfNull(viewJson);

        var key = MakeKey(storageKey, instanceId);
        _entries.AddOrUpdate(
            key,
            _ => new Entry(viewJson, sequence),
            (_, existing) =>
            {
                // Keep newer memory; do not clobber with an older snapshot.
                if (existing.Sequence > sequence)
                    return existing;
                existing.Update(viewJson, sequence);
                return existing;
            });

        EvictIfNeeded();
    }

    /// <inheritdoc />
    public void OfferFromDurable(string storageKey, string instanceId, string viewJson, long sequence)
    {
        if (_mode == ProjectionReadCacheMode.Off)
            return;

        ArgumentException.ThrowIfNullOrWhiteSpace(storageKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        ArgumentNullException.ThrowIfNull(viewJson);

        var key = MakeKey(storageKey, instanceId);

        // Already resident with equal-or-newer seq — nothing to promote.
        if (_entries.TryGetValue(key, out var existing) && existing.Sequence >= sequence)
            return;

        var threshold = _mode == ProjectionReadCacheMode.Resident ? 1 : _promoteAfterHits;
        var hits = _durableHits.AddOrUpdate(key, 1, (_, prev) => prev + 1);
        if (hits < threshold)
            return;

        Set(storageKey, instanceId, viewJson, sequence);
        _durableHits.TryRemove(key, out _);
    }

    /// <inheritdoc />
    public void Remove(string storageKey, string instanceId)
    {
        var key = MakeKey(storageKey, instanceId);
        _entries.TryRemove(key, out _);
        _durableHits.TryRemove(key, out _);
    }

    /// <inheritdoc />
    public void Clear(string storageKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storageKey);
        var prefix = storageKey + "\u001f";

        foreach (var key in _entries.Keys)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
                _entries.TryRemove(key, out _);
        }

        foreach (var key in _durableHits.Keys)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
                _durableHits.TryRemove(key, out _);
        }
    }

    /// <inheritdoc />
    public void ClearAll()
    {
        _entries.Clear();
        _durableHits.Clear();
    }

    /// <summary>Current entry count (tests / diagnostics).</summary>
    public int Count => _entries.Count;

    private void EvictIfNeeded()
    {
        var overflow = _entries.Count - _maxEntries;
        if (overflow <= 0)
            return;

        // Approximate LRU: drop the coldest entries. Safe — cache is an accelerator only.
        foreach (var victim in _entries
                     .OrderBy(static kv => Volatile.Read(ref kv.Value.LastAccessTicks))
                     .Take(overflow)
                     .Select(static kv => kv.Key)
                     .ToList())
        {
            _entries.TryRemove(victim, out _);
            _durableHits.TryRemove(victim, out _);
        }
    }

    private static string MakeKey(string storageKey, string instanceId) =>
        string.Concat(storageKey, "\u001f", instanceId);

    private sealed class Entry
    {
        public string ViewJson;
        public long Sequence;
        public long LastAccessTicks;

        public Entry(string viewJson, long sequence)
        {
            ViewJson = viewJson;
            Sequence = sequence;
            LastAccessTicks = Environment.TickCount64;
        }

        public void Update(string viewJson, long sequence)
        {
            ViewJson = viewJson;
            Sequence = sequence;
            Touch();
        }

        public void Touch() => LastAccessTicks = Environment.TickCount64;
    }
}
