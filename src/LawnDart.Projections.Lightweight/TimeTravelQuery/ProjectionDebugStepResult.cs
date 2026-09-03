namespace LawnDart.Projections.Lightweight.TimeTravelQuery;

/// <summary>
/// The result of a single step-forward or step-back operation on a projection instance.
/// </summary>
public sealed record ProjectionDebugStepResult
{
    /// <summary>
    /// The 0-based applied index of the new cursor position after the step.
    /// <c>-1</c> indicates the cursor is before the first event (empty state).
    /// </summary>
    public required long AppliedIndex { get; init; }

    /// <summary>
    /// The event that was applied at <see cref="AppliedIndex"/>.
    /// <see langword="null"/> when the cursor is at the empty-state position (<c>AppliedIndex = -1</c>)
    /// or when the projection has no events.
    /// </summary>
    public required AppliedEventEntry? Event { get; init; }

    /// <summary>The full view state at the new cursor position.</summary>
    public required TimeTravelResult State { get; init; }

    /// <summary>
    /// The structural diff between the state <em>before</em> the step and the state
    /// <em>after</em> the step. <see langword="null"/> only when the API does not have a previous
    /// state to compare for the requested cursor position.
    /// </summary>
    public required ViewStateDiff? Diff { get; init; }
}
