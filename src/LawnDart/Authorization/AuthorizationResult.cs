namespace LawnDart.Authorization;

/// <summary>
/// Result of an authorization check.
/// </summary>
public class AuthorizationResult
{
    /// <summary>
    /// Whether the authorization check succeeded.
    /// </summary>
    public bool IsAuthorized { get; init; }

    /// <summary>
    /// Reason for authorization failure (null if authorized).
    /// </summary>
    public string? FailureReason { get; init; }

    /// <summary>
    /// List of failed checks (permission names, entitlement names, policy names).
    /// </summary>
    public IReadOnlyList<string> FailedChecks { get; init; } = Array.Empty<string>();
    
    /// <summary>
    /// Creates a successful authorization result.
    /// </summary>
    public static AuthorizationResult Success() 
        => new() { IsAuthorized = true };
    
    /// <summary>
    /// Creates a failed authorization result.
    /// </summary>
    /// <param name="reason">Reason for failure.</param>
    /// <param name="failedChecks">List of failed checks.</param>
    public static AuthorizationResult Failure(string reason, params string[] failedChecks) 
        => new() 
        { 
            IsAuthorized = false, 
            FailureReason = reason, 
            FailedChecks = failedChecks 
        };
}
