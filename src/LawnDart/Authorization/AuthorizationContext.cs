namespace LawnDart.Authorization;

/// <summary>
/// Represents the authorization context captured from the transport layer.
/// Different transports (HTTP, messaging, background) populate this differently.
/// </summary>
public class AuthorizationContext
{
    // User Identity
    /// <summary>
    /// User who is executing the command.
    /// </summary>
    public string? UserId { get; set; }

    /// <summary>
    /// User display name.
    /// </summary>
    public string? UserName { get; set; }

    /// <summary>
    /// User roles assigned to the user.
    /// </summary>
    public IReadOnlyList<string> UserRoles { get; set; } = Array.Empty<string>();

    /// <summary>
    /// User claims (can include permissions like "permission:Orders.Create").
    /// </summary>
    public IReadOnlyList<string> UserClaims { get; set; } = Array.Empty<string>();
    
    // Account/Tenant Context
    /// <summary>
    /// Tenant/organization context.
    /// </summary>
    public string? TenantId { get; set; }

    /// <summary>
    /// Account/billing context.
    /// </summary>
    public string? AccountId { get; set; }

    /// <summary>
    /// Account-level entitlements (features enabled for the account).
    /// </summary>
    public IReadOnlyList<string> AccountEntitlements { get; set; } = Array.Empty<string>();
    
    // Transport Context
    /// <summary>
    /// Type of transport that captured this context (e.g., "HTTP", "ServiceBus", "BackgroundService").
    /// </summary>
    public string? TransportType { get; set; }

    /// <summary>
    /// Additional custom context key-value pairs.
    /// </summary>
    public Dictionary<string, string> CustomContext { get; set; } = new();
    
    // Tracing (for audit)
    /// <summary>
    /// Correlation ID for tracing the request.
    /// </summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// When this context was captured.
    /// </summary>
    public DateTime CapturedAt { get; set; } = DateTime.UtcNow;
}
