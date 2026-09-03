using MemoryPack;

namespace LawnDart.Metadata;

/// <summary>
/// Metadata for an event, including identity, network context, tracing, authorization, and schema information.
/// </summary>
[MemoryPackable]
public partial class EventMetadata
{
    // Identity (inherited from command)
    /// <summary>
    /// User who initiated the command that caused this event.
    /// </summary>
    public string? UserId { get; set; }

    /// <summary>
    /// User display name.
    /// </summary>
    public string? UserName { get; set; }

    /// <summary>
    /// Tenant/organization context.
    /// </summary>
    public string? TenantId { get; set; }

    /// <summary>
    /// Account/billing context.
    /// </summary>
    public string? AccountId { get; set; }

    // Network Context (inherited from command)
    /// <summary>
    /// Client IP address.
    /// </summary>
    public string? IpAddress { get; set; }

    /// <summary>
    /// User agent string.
    /// </summary>
    public string? UserAgent { get; set; }

    // Tracing (inherited and extended)
    /// <summary>
    /// Request correlation ID.
    /// </summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// Command ID that caused this event.
    /// </summary>
    public string? CausationId { get; set; }

    /// <summary>
    /// Unique event ID.
    /// </summary>
    public string EventId { get; set; } = Guid.NewGuid().ToString();

    // Authorization Context (inherited from command)
    /// <summary>
    /// Who authorized the command.
    /// </summary>
    public string? AuthorizedBy { get; set; }

    /// <summary>
    /// When authorization occurred.
    /// </summary>
    public DateTime? AuthorizedAt { get; set; }

    /// <summary>
    /// Policies that authorized the command.
    /// </summary>
    public string[]? AuthorizationPolicies { get; set; }

    // Timing
    /// <summary>
    /// When event was created (business/domain time).
    /// This represents when the domain event actually occurred.
    /// </summary>
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// When event was committed to the event store (system/infrastructure time).
    /// Set automatically by the event store during AppendAsync.
    /// Used for measuring projection lag and system performance.
    /// </summary>
    public DateTime? CommitTimestamp { get; set; }

    // Schema
    /// <summary>
    /// Event schema version.
    /// </summary>
    public int SchemaVersion { get; set; } = 1;

    /// <summary>
    /// Event schema name.
    /// </summary>
    public string? SchemaName { get; set; }

    // Custom
    /// <summary>
    /// Additional custom context.
    /// </summary>
    public Dictionary<string, string> Custom { get; set; } = new();
}


