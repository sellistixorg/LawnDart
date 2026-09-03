namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// Controls how the Lightweight runner loads projection instances into RAM at startup.
/// </summary>
public enum ProjectionWorkingSetMode
{
    /// <summary>
    /// Warm-restore all owned views from the durable store on start (historical default).
    /// </summary>
    EagerRestore = 0,

    /// <summary>
    /// Skip bulk restore; hydrate each instance from durable storage on first event (or GET via SQL).
    /// </summary>
    Lazy = 1
}
