namespace LawnDart.Outbox;

/// <summary>
/// Represents an event message waiting to be published to external systems.
/// Part of the transactional outbox pattern for reliable event publishing.
/// </summary>
public class OutboxMessage
{
    /// <summary>
    /// Unique identifier for this outbox message.
    /// </summary>
    public required Guid Id { get; init; }
    
    /// <summary>
    /// Event type name (fully qualified type name).
    /// </summary>
    public required string EventType { get; init; }
    
    /// <summary>
    /// Serialized event payload (JSON).
    /// </summary>
    public required string Payload { get; init; }
    
    /// <summary>
    /// Serialized event metadata (JSON).
    /// </summary>
    public required string Metadata { get; init; }
    
    /// <summary>
    /// Timestamp when the message was created.
    /// </summary>
    public required DateTime CreatedAt { get; init; }
    
    /// <summary>
    /// Timestamp when the message was processed (null if not yet processed).
    /// </summary>
    public DateTime? ProcessedAt { get; set; }
    
    /// <summary>
    /// Number of times processing has been attempted.
    /// </summary>
    public int Attempts { get; set; }
    
    /// <summary>
    /// Last error message (null if no error).
    /// </summary>
    public string? LastError { get; set; }
    
    /// <summary>
    /// Timestamp of the last attempt.
    /// </summary>
    public DateTime? LastAttemptAt { get; set; }

    /// <summary>
    /// Timestamp when the message was dead-lettered after exceeding max publish attempts.
    /// Null if the message has not been dead-lettered. Distinct from <see cref="ProcessedAt"/>
    /// (successful publish).
    /// </summary>
    public DateTime? DeadLetteredAt { get; set; }
    
    /// <summary>
    /// Stream ID the event belongs to (for correlation).
    /// </summary>
    public required string StreamId { get; init; }
    
    /// <summary>
    /// Sequence position in the event store (for ordering).
    /// </summary>
    public required long SequencePosition { get; init; }
}
