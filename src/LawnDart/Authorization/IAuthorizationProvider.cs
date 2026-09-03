namespace LawnDart.Authorization;

/// <summary>
/// Performs authorization checks based on the authorization context.
/// Supports both user-level permissions and account-level entitlements.
/// </summary>
public interface IAuthorizationProvider
{
    /// <summary>
    /// Checks if the authorization context has the required user-level permission.
    /// </summary>
    /// <param name="context">Authorization context containing user identity and claims.</param>
    /// <param name="permission">Permission name to check (e.g., "Orders.Create").</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Authorization result indicating success or failure.</returns>
    Task<AuthorizationResult> CheckPermissionAsync(
        AuthorizationContext context, 
        string permission,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Checks if the authorization context has the required account-level entitlement.
    /// </summary>
    /// <param name="context">Authorization context containing account information.</param>
    /// <param name="entitlement">Entitlement name to check (e.g., "PremiumFeatures").</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Authorization result indicating success or failure.</returns>
    Task<AuthorizationResult> CheckEntitlementAsync(
        AuthorizationContext context, 
        string entitlement,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Checks if the authorization context satisfies a custom policy.
    /// </summary>
    /// <param name="context">Authorization context.</param>
    /// <param name="policyName">Policy name to check (e.g., "CanAccessTenant").</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Authorization result indicating success or failure.</returns>
    Task<AuthorizationResult> CheckPolicyAsync(
        AuthorizationContext context, 
        string policyName,
        CancellationToken cancellationToken = default);
}
