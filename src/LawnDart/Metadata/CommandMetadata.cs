namespace LawnDart.Metadata;

/// <summary>
/// Metadata captured for a command, including identity, network context, tracing, and authorization information.
/// </summary>
public class CommandMetadata
{
    // Identity
    /// <summary>
    /// User who initiated the command.
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

    // Network Context
    /// <summary>
    /// Client IP address.
    /// </summary>
    public string? IpAddress { get; set; }

    /// <summary>
    /// User agent string.
    /// </summary>
    public string? UserAgent { get; set; }

    // Tracing
    /// <summary>
    /// Request correlation ID.
    /// </summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// Previous command/event ID that caused this command.
    /// </summary>
    public string? CausationId { get; set; }

    // Authorization Context
    /// <summary>
    /// Who authorized the command (if different from user).
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
    /// When command was received.
    /// </summary>
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;

    // Custom
    /// <summary>
    /// Additional custom context.
    /// </summary>
    public Dictionary<string, string> Custom { get; set; } = new();
}


