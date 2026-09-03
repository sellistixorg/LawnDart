namespace LawnDart.Projections.Lightweight.TimeTravelQuery;

/// <summary>
/// A paged slice of the applied-event timeline for a specific projection instance.
/// </summary>
/// <remarks>
/// The <see cref="TotalApplied"/> count reflects only the events that were scanned
/// and matched by the projection's filtering rules during this request — it is the
/// total number of events the projection instance has ever applied, not the total
/// number of events in the event store.
/// </remarks>
public sealed record ProjectionTimeline
{
    /// <summary>The logical projection family name.</summary>
    public required string LogicalName { get; init; }

    /// <summary>The projection implementation version.</summary>
    public required int ProjectionVersion { get; init; }

    /// <summary>The internal storage key used for checkpoints and stored views.</summary>
    public required string ProjectionStorageKey { get; init; }

    /// <summary>Gets the compatibility alias for the logical projection family name.</summary>
    public string ProjectionName => LogicalName;

    /// <summary>Gets a value indicating whether this version owns the latest/default alias.</summary>
    public required bool IsLatest { get; init; }

    /// <summary>Gets how latest/default resolution was determined for the logical family.</summary>
    public required string LatestResolutionMode { get; init; }

    /// <summary>The view instance key (stream ID, entity key, or "global").</summary>
    public required string InstanceId { get; init; }

    /// <summary>The applied-event entries in this page, ordered by <see cref="AppliedEventEntry.AppliedIndex"/>.</summary>
    public required IReadOnlyList<AppliedEventEntry> Events { get; init; }

    /// <summary>The 0-based applied index of the first entry in <see cref="Events"/>.</summary>
    public required int FromIndex { get; init; }

    /// <summary>The requested maximum page size.</summary>
    public required int PageSize { get; init; }

    /// <summary>
    /// The total number of events this projection instance has applied (regardless of page).
    /// <c>-1</c> when the total count was not computed (e.g. the scan was stopped early).
    /// </summary>
    public required long TotalApplied { get; init; }
}
