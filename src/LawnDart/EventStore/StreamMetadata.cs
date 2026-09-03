namespace LawnDart.EventStore;

/// <summary>
/// Metadata about an event stream.
/// </summary>
public class StreamMetadata
{
    /// <summary>
    /// Unique stream identifier (e.g., "tenant-123:Order:order-456" or "tenant-123:Customer:customer-789").
    /// Format: {tenantId}:{aggregateType}:{aggregateId}
    /// </summary>
    public string StreamId { get; set; } = string.Empty;

    /// <summary>
    /// Tenant ID extracted from stream ID.
    /// </summary>
    public string? TenantId { get; set; }

    /// <summary>
    /// Aggregate type name (e.g., "Order", "Customer").
    /// Extracted from stream ID or inferred from events.
    /// </summary>
    public string AggregateType { get; set; } = string.Empty;

    /// <summary>
    /// Aggregate identifier (Guid) if extractable from stream ID.
    /// </summary>
    public Guid? AggregateId { get; set; }

    /// <summary>
    /// Current version of the stream (highest version number).
    /// </summary>
    public long CurrentVersion { get; set; }

    /// <summary>
    /// Last global sequence position for events in this stream.
    /// </summary>
    public long LastSequencePosition { get; set; }

    /// <summary>
    /// Timestamp when the stream was created (first event).
    /// </summary>
    public DateTime CreatedAt { get; set; }

    /// <summary>
    /// Timestamp of the most recent event in the stream.
    /// </summary>
    public DateTime LastEventAt { get; set; }

    /// <summary>
    /// Total number of events in the stream.
    /// </summary>
    public long EventCount { get; set; }

    /// <summary>
    /// Common tags associated with this stream.
    /// Updated when new tags are added to events.
    /// </summary>
    public IReadOnlyList<string> Tags { get; set; } = Array.Empty<string>();

    /// <summary>
    /// Current status of the stream.
    /// </summary>
    public StreamStatus Status { get; set; } = StreamStatus.Active;
}


