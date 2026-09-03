using System.Text.Json;

namespace LawnDart.Projections.Lightweight.TimeTravelQuery;

/// <summary>
/// Describes a single event in a projection instance's applied-event timeline,
/// including the full <see cref="LawnDart.Metadata.EventMetadata"/> fields and any DCB tags.
/// </summary>
/// <remarks>
/// The <see cref="AppliedIndex"/> is 0-based and is scoped to the specific
/// projection instance — it is <em>not</em> the global sequence position.
/// Two different projection instances (or two different projection names) have
/// independent applied-index counters.
/// </remarks>
public sealed record AppliedEventEntry
{
    /// <summary>
    /// The 0-based position of this event within the applied-event timeline for this
    /// projection instance. The first event ever applied has <c>AppliedIndex = 0</c>.
    /// </summary>
    public required long AppliedIndex { get; init; }

    /// <summary>The global sequence position assigned by the event store.</summary>
    public required long SequencePosition { get; init; }

    /// <summary>The stream ID the event was written to.</summary>
    public required string StreamId { get; init; }

    /// <summary>The version of this event within its stream.</summary>
    public required long Version { get; init; }

    /// <summary>The domain timestamp recorded in the event metadata.</summary>
    public required DateTime Timestamp { get; init; }

    /// <summary>The fully-qualified or short event type name.</summary>
    public required string EventType { get; init; }

    /// <summary>The serialized event payload for this applied event.</summary>
    public required JsonElement? EventJson { get; init; }

    // ── EventMetadata fields ──────────────────────────────────────────────────

    /// <summary>Unique event ID from <c>EventMetadata.EventId</c>.</summary>
    public string? EventId { get; init; }

    /// <summary>User who triggered the command that produced this event.</summary>
    public string? UserId { get; init; }

    /// <summary>Display name of the user who triggered the command.</summary>
    public string? UserName { get; init; }

    /// <summary>Tenant/organisation context from event metadata.</summary>
    public string? TenantId { get; init; }

    /// <summary>Request correlation ID (spans a full user request).</summary>
    public string? CorrelationId { get; init; }

    /// <summary>Causation ID — the command/event that directly caused this event.</summary>
    public string? CausationId { get; init; }

    /// <summary>
    /// Wall-clock time the event was committed to the event store (set by the store on append).
    /// May be <see langword="null"/> for events written by older store versions.
    /// </summary>
    public DateTime? CommitTimestamp { get; init; }

    /// <summary>Client IP address captured at command dispatch time.</summary>
    public string? IpAddress { get; init; }

    /// <summary>DCB tags associated with the event (empty for stream-based events).</summary>
    public IReadOnlyList<string> Tags { get; init; } = [];
}
