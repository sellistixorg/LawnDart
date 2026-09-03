namespace LawnDart.Projections.Lightweight.Registration;

/// <summary>
/// Identifies the structural kind of a lightweight projection, which determines the event
/// reading strategy and instance lifecycle used by
/// <see cref="LawnDart.Projections.Lightweight.Hosting.LightweightProjectionRunnerService"/>.
/// </summary>
public enum ProjectionKind
{
    /// <summary>
    /// Per-stream projection: one view instance per stream of the specified aggregate type.
    /// Events are filtered to streams matching the <c>StreamType</c>.
    /// </summary>
    SingleStream,

    /// <summary>
    /// Global projection: a single view instance built from the complete global event sequence.
    /// </summary>
    Global,

    /// <summary>
    /// DCB projection: a single view instance built from a query-filtered subset of the global sequence.
    /// </summary>
    Dcb,

    /// <summary>
    /// Multi-stream projection: one view instance per logical entity, built from a query-filtered
    /// subset of the global sequence spanning multiple aggregate stream types. The handler must also
    /// implement <see cref="LawnDart.Projections.Lightweight.IMultiStreamEntityResolver"/>
    /// to map incoming events to their target entity instance.
    /// </summary>
    MultiStream
}
