namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// Per-process hot read accelerator for projection views.
/// Keyed by bounded context in DI when using <c>WithProjections</c>.
/// </summary>
/// <remarks>
/// Memory is an accelerator only — durable <c>IViewStore</c> remains system of record.
/// Cache miss must never become a 404 when SQL still has the row.
/// </remarks>
public interface IProjectionReadCache
{
    /// <summary>Attempts to read a cached view for <paramref name="storageKey"/> / <paramref name="instanceId"/>.</summary>
    bool TryGet(string storageKey, string instanceId, out ProjectionCachedView view);

    /// <summary>
    /// Upserts a view snapshot. Ignores the write when an existing entry has a
    /// <em>higher</em> sequence (keeps memory ahead of a lagging durable promote).
    /// </summary>
    void Set(string storageKey, string instanceId, string viewJson, long sequence);

    /// <summary>
    /// Records a durable-store GET hit and may promote into the cache depending on
    /// <see cref="ProjectionReadCacheMode"/>.
    /// </summary>
    void OfferFromDurable(string storageKey, string instanceId, string viewJson, long sequence);

    /// <summary>Removes one instance entry.</summary>
    void Remove(string storageKey, string instanceId);

    /// <summary>Clears all entries for a projection storage key (rebuild / delete).</summary>
    void Clear(string storageKey);

    /// <summary>Removes every entry.</summary>
    void ClearAll();
}

/// <summary>Cached projection view payload used by <see cref="IProjectionReadCache"/>.</summary>
public readonly record struct ProjectionCachedView(string ViewJson, long Sequence);
