namespace LawnDart.Authorization;

/// <summary>
/// Specifies that a command requires a user-level permission to execute.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
public class RequiresPermissionAttribute : Attribute
{
    /// <summary>
    /// The permission name required to execute the command.
    /// </summary>
    public string Permission { get; }
    
    /// <summary>
    /// Initializes a new instance of the RequiresPermissionAttribute.
    /// </summary>
    /// <param name="permission">Permission name (e.g., "Orders.Create").</param>
    public RequiresPermissionAttribute(string permission)
    {
        Permission = permission ?? throw new ArgumentNullException(nameof(permission));
    }
}

/// <summary>
/// Specifies that a command requires an account-level entitlement to execute.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
public class RequiresEntitlementAttribute : Attribute
{
    /// <summary>
    /// The entitlement name required to execute the command.
    /// </summary>
    public string Entitlement { get; }
    
    /// <summary>
    /// Initializes a new instance of the RequiresEntitlementAttribute.
    /// </summary>
    /// <param name="entitlement">Entitlement name (e.g., "PremiumFeatures").</param>
    public RequiresEntitlementAttribute(string entitlement)
    {
        Entitlement = entitlement ?? throw new ArgumentNullException(nameof(entitlement));
    }
}

/// <summary>
/// Specifies that a command requires a custom authorization policy.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
public class RequiresPolicyAttribute : Attribute
{
    /// <summary>
    /// The policy name required to execute the command.
    /// </summary>
    public string PolicyName { get; }
    
    /// <summary>
    /// Initializes a new instance of the RequiresPolicyAttribute.
    /// </summary>
    /// <param name="policyName">Policy name (e.g., "CanAccessTenant").</param>
    public RequiresPolicyAttribute(string policyName)
    {
        PolicyName = policyName ?? throw new ArgumentNullException(nameof(policyName));
    }
}
