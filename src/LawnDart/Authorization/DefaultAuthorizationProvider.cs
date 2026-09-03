using Microsoft.Extensions.Logging;

namespace LawnDart.Authorization;

/// <summary>
/// Default authorization provider with simple permission and entitlement checks.
/// Can be extended or replaced with custom logic (LDAP, OAuth, external service, etc.).
/// </summary>
public class DefaultAuthorizationProvider : IAuthorizationProvider
{
    private readonly ILogger<DefaultAuthorizationProvider>? _logger;
    
    /// <summary>
    /// Initializes a new instance of the DefaultAuthorizationProvider.
    /// </summary>
    /// <param name="logger">Optional logger.</param>
    public DefaultAuthorizationProvider(ILogger<DefaultAuthorizationProvider>? logger = null)
    {
        _logger = logger;
    }
    
    /// <summary>
    /// Checks if the user has the required permission.
    /// </summary>
    public virtual Task<AuthorizationResult> CheckPermissionAsync(
        AuthorizationContext context, 
        string permission,
        CancellationToken cancellationToken = default)
    {
        if (context == null)
            return Task.FromResult(AuthorizationResult.Failure("No authorization context"));
        
        // Check if user has the permission via claims
        var hasPermission = context.UserClaims.Any(c => 
            c.Equals($"permission:{permission}", StringComparison.OrdinalIgnoreCase) ||
            c.Equals($"permissions:{permission}", StringComparison.OrdinalIgnoreCase));
        
        if (!hasPermission)
        {
            // Check if any role grants the permission (example: "Admin" role has all permissions)
            hasPermission = context.UserRoles.Contains("Admin", StringComparer.OrdinalIgnoreCase);
        }
        
        if (hasPermission)
        {
            _logger?.LogDebug(
                "User {UserId} authorized for permission {Permission}",
                context.UserId,
                permission);
            return Task.FromResult(AuthorizationResult.Success());
        }
        
        _logger?.LogWarning(
            "User {UserId} denied permission {Permission}",
            context.UserId,
            permission);
        
        return Task.FromResult(AuthorizationResult.Failure(
            $"User lacks required permission: {permission}",
            permission));
    }
    
    /// <summary>
    /// Checks if the account has the required entitlement.
    /// </summary>
    public virtual Task<AuthorizationResult> CheckEntitlementAsync(
        AuthorizationContext context, 
        string entitlement,
        CancellationToken cancellationToken = default)
    {
        if (context == null)
            return Task.FromResult(AuthorizationResult.Failure("No authorization context"));
        
        // Check if account has the entitlement
        var hasEntitlement = context.AccountEntitlements.Contains(entitlement, StringComparer.OrdinalIgnoreCase);
        
        if (hasEntitlement)
        {
            _logger?.LogDebug(
                "Account {AccountId} has entitlement {Entitlement}",
                context.AccountId,
                entitlement);
            return Task.FromResult(AuthorizationResult.Success());
        }
        
        _logger?.LogWarning(
            "Account {AccountId} denied entitlement {Entitlement}",
            context.AccountId,
            entitlement);
        
        return Task.FromResult(AuthorizationResult.Failure(
            $"Account lacks required entitlement: {entitlement}",
            entitlement));
    }
    
    /// <summary>
    /// Checks if the context satisfies a custom policy.
    /// Override this for custom policy logic.
    /// </summary>
    public virtual Task<AuthorizationResult> CheckPolicyAsync(
        AuthorizationContext context, 
        string policyName,
        CancellationToken cancellationToken = default)
    {
        // Override this for custom policy logic
        // Example policies: "CanAccessTenant", "MustBeAccountOwner", etc.
        
        _logger?.LogWarning("Policy check not implemented: {PolicyName}", policyName);
        return Task.FromResult(AuthorizationResult.Failure($"Policy not implemented: {policyName}"));
    }
}
