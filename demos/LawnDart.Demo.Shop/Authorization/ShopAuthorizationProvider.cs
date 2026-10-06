using Microsoft.Extensions.Logging;
using LawnDart.Authorization;

namespace LawnDart.Demo.Shop.Authorization;

/// <summary>
/// Maps the five hardcoded demo users to their role-based permissions.
///
/// Alice  = Customer (tenant: buyers): browse products, place orders, view own orders.
/// Maria  = Customer (tenant: buyers): same as Alice.
/// Bob    = Seller (tenant: bobs-store): create and update products, view sales, manage inventory.
/// Larry  = Seller (tenant: larrys-emporium): same as Bob.
/// Carol  = Warehouse (tenant: system): ship orders, view inventory.
/// </summary>
public sealed class ShopAuthorizationProvider : DefaultAuthorizationProvider
{
    private static readonly Dictionary<string, HashSet<string>> RolePermissions =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["Customer"] = new(StringComparer.OrdinalIgnoreCase)
            {
                ShopPermissions.OrderPlace,
                ShopPermissions.OrderView,
                ShopPermissions.ProductView
            },
            ["Seller"] = new(StringComparer.OrdinalIgnoreCase)
            {
                ShopPermissions.ProductCreate,
                ShopPermissions.ProductUpdate,
                ShopPermissions.ProductView,
                ShopPermissions.OrderView,
                ShopPermissions.OrderCancel,
                ShopPermissions.InventoryView
            },
            ["Warehouse"] = new(StringComparer.OrdinalIgnoreCase)
            {
                ShopPermissions.OrderShip,
                ShopPermissions.OrderView,
                ShopPermissions.InventoryView
            }
        };

    public ShopAuthorizationProvider(ILoggerFactory? loggerFactory = null)
        : base(loggerFactory?.CreateLogger<DefaultAuthorizationProvider>()) { }

    public override async Task<AuthorizationResult> CheckPermissionAsync(
        AuthorizationContext context,
        string permission,
        CancellationToken cancellationToken = default)
    {
        foreach (var role in context.UserRoles)
        {
            if (RolePermissions.TryGetValue(role, out var granted) &&
                granted.Contains(permission))
            {
                return AuthorizationResult.Success();
            }
        }

        return await base.CheckPermissionAsync(context, permission, cancellationToken);
    }
}
