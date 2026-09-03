namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// Controls whether projection GET endpoints may serve / populate an in-process read accelerator.
/// </summary>
public enum ProjectionReadCacheMode
{
    /// <summary>Do not read or write the hot cache; durable store only.</summary>
    Off = 0,

    /// <summary>
    /// Serve memory when present; promote durable hits into the cache after
    /// <see cref="LightweightProjectionOptions.ReadCachePromoteAfterHits"/> GETs.
    /// </summary>
    Hot = 1,

    /// <summary>
    /// Like <see cref="Hot"/>, but durable misses promote on the first GET (aggressive residency).
    /// </summary>
    Resident = 2
}
