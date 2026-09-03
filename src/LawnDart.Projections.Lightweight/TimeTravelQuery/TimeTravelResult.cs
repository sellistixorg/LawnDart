namespace LawnDart.Projections.Lightweight.TimeTravelQuery;

/// <summary>
/// The result of an ad-hoc point-in-time projection replay via <see cref="AdHocProjectionBuilder"/>.
/// </summary>
public sealed record TimeTravelResult
{
    /// <summary>The logical projection family that was replayed.</summary>
    public required string LogicalName { get; init; }

    /// <summary>The projection implementation version that was replayed.</summary>
    public required int ProjectionVersion { get; init; }

    /// <summary>The internal storage key used for checkpoints and stored views.</summary>
    public required string ProjectionStorageKey { get; init; }

    /// <summary>Gets the compatibility alias for the logical projection family name.</summary>
    public string ProjectionName => LogicalName;

    /// <summary>Gets a value indicating whether this version owns the latest/default alias.</summary>
    public required bool IsLatest { get; init; }

    /// <summary>Gets how latest/default resolution was determined for the logical family.</summary>
    public required string LatestResolutionMode { get; init; }

    /// <summary>The instance key for which the projection was replayed (stream ID, entity key, or "global").</summary>
    public required string InstanceId { get; init; }

    /// <summary>The serialized JSON view state at the requested point in time.</summary>
    public required string ViewJson { get; init; }

    /// <summary>The total number of events that were applied to reach this state.</summary>
    public required long EventsApplied { get; init; }

    /// <summary>The global sequence position of the last event applied, or <c>null</c> if no events were applied.</summary>
    public required long? FinalSequencePosition { get; init; }

    /// <summary>The stream version of the last event applied, or <c>null</c> if no events were applied.</summary>
    public required long? FinalStreamVersion { get; init; }

    /// <summary>The domain timestamp of the last event applied, or <c>null</c> if no events were applied.</summary>
    public required DateTime? FinalEventTimestamp { get; init; }

    /// <summary>Human-readable description of the cutoff that was requested.</summary>
    public required string RequestedCutoff { get; init; }

    /// <summary>The projection kind (SingleStream, Global, Dcb, or MultiStream).</summary>
    public required string ProjectionKind { get; init; }
}
