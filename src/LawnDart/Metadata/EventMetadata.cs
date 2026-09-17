namespace LawnDart.Metadata;

/// <summary>
/// Metadata for an event, including identity, network context, tracing, authorization, and schema information.
/// </summary>
/// <remarks>
/// Three clocks, do not mix them:
/// <list type="bullet">
/// <item><description><b>Business time</b> — <c>IEvent.Timestamp</c> and
/// <see cref="Timestamp"/> are the same after enrich. Time-travel
/// (<c>toTimestamp</c>) uses this value.</description></item>
/// <item><description><b>Commit time</b> — first-class on the recorded
/// event; <see cref="CommitTimestamp"/> is a session mirror. Used for lag,
/// not domain queries.</description></item>
/// <item><description><b>Trace</b> — <see cref="TraceId"/> / <see cref="SpanId"/>
/// (W3C hex). The Activity clock is not stored as a third <see cref="DateTime"/>.</description></item>
/// </list>
/// </remarks>
public class EventMetadata
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
    /// Business/domain time — when the event occurred.
    /// Copied from <c>IEvent.Timestamp</c> during enrich. Not overwritten with
    /// <see cref="DateTime.UtcNow"/>. Time-travel filters use this value.
    /// </summary>
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// When the event was committed (system/infrastructure time).
    /// Compatibility mirror of <c>RecordedEvent.CommitTimestamp</c> written by
    /// the typed session on hydrate. The frame is authority. Used for lag, not
    /// domain queries. Stored metadata JSON is not required to contain this.
    /// </summary>
    public DateTime? CommitTimestamp { get; set; }

    // Schema
    /// <summary>
    /// Event schema version. Compatibility mirror of the log-frame
    /// <c>SchemaVersion</c> written by the typed session on append and hydrate.
    /// The frame / SQL column is authority. Missing or zero on old rows is 1.
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

    /// <summary>
    /// W3C trace id (32 hex characters). Inherited from the command envelope.
    /// </summary>
    public string? TraceId { get; set; }

    /// <summary>
    /// W3C span id (16 hex characters). Inherited from the command envelope.
    /// </summary>
    public string? SpanId { get; set; }
}


