namespace LawnDart.Projections.Lightweight.TimeTravelQuery;

/// <summary>
/// Discriminates the three supported point-in-time cutoff kinds for ad-hoc projection replay.
/// </summary>
public enum CutoffKind
{
    /// <summary>Replay events up to and including the specified global sequence position.</summary>
    Sequence,

    /// <summary>Replay events up to and including the specified stream version (SingleStream only).</summary>
    Version,

    /// <summary>Replay events whose <c>EventMetadata.Timestamp</c> is on or before the specified value.</summary>
    Timestamp
}

/// <summary>
/// Specifies a point-in-time boundary for ad-hoc projection replay.
/// Create instances via the static factory methods: <see cref="AtSequence"/>,
/// <see cref="AtVersion"/>, or <see cref="AtTimestamp"/>.
/// </summary>
public sealed record ProjectionCutoff
{
    /// <summary>The kind of cutoff this instance represents.</summary>
    public CutoffKind Kind { get; private init; }

    /// <summary>The global sequence position upper bound (inclusive). Set when <see cref="Kind"/> is <see cref="CutoffKind.Sequence"/>.</summary>
    public long? SequencePosition { get; private init; }

    /// <summary>The stream version upper bound (inclusive). Set when <see cref="Kind"/> is <see cref="CutoffKind.Version"/>.</summary>
    public long? Version { get; private init; }

    /// <summary>The timestamp upper bound (inclusive). Set when <see cref="Kind"/> is <see cref="CutoffKind.Timestamp"/>.</summary>
    public DateTime? Timestamp { get; private init; }

    /// <summary>
    /// Replay events up to and including the specified global sequence position.
    /// Supported by all projection kinds.
    /// </summary>
    public static ProjectionCutoff AtSequence(long position) =>
        new() { Kind = CutoffKind.Sequence, SequencePosition = position };

    /// <summary>
    /// Replay events up to and including the specified stream version.
    /// Only valid for <see cref="CutoffKind.Version"/> cutoffs on SingleStream projections.
    /// </summary>
    public static ProjectionCutoff AtVersion(long version) =>
        new() { Kind = CutoffKind.Version, Version = version };

    /// <summary>
    /// Replay events whose <c>EventMetadata.Timestamp</c> is on or before the specified UTC timestamp.
    /// Supported by all projection kinds.
    /// </summary>
    public static ProjectionCutoff AtTimestamp(DateTime timestamp) =>
        new() { Kind = CutoffKind.Timestamp, Timestamp = timestamp };

    /// <summary>A human-readable description of this cutoff, suitable for logging and MCP responses.</summary>
    public string Description => Kind switch
    {
        CutoffKind.Sequence  => $"sequence <= {SequencePosition}",
        CutoffKind.Version   => $"stream version <= {Version}",
        CutoffKind.Timestamp => $"timestamp <= {Timestamp:O}",
        _                    => "unknown"
    };
}
