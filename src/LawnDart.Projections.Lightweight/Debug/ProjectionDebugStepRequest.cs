namespace LawnDart.Projections.Lightweight.Debug;

/// <summary>
/// Request payload for stepping forward or backward through a projection instance timeline.
/// </summary>
public sealed record ProjectionDebugStepRequest
{
    /// <summary>
    /// Gets or sets the step direction. Expected values are <c>"forward"</c> or <c>"back"</c>.
    /// </summary>
    public string Direction { get; init; } = "forward";

    /// <summary>
    /// Gets or sets the current 0-based applied-event index.
    /// </summary>
    public long CurrentAppliedIndex { get; init; }
}
