namespace LawnDart.Demo.Shop.Authorization;

/// <summary>
/// Permission names checked on shop commands.
/// </summary>
public static class ShopPermissions
{
    /// <summary>Create a product.</summary>
    public const string ProductCreate = "Product.Create";

    /// <summary>Change price or stock.</summary>
    public const string ProductUpdate = "Product.Update";

    /// <summary>Read the catalog.</summary>
    public const string ProductView   = "Product.View";

    /// <summary>Place an order.</summary>
    public const string OrderPlace  = "Order.Place";

    /// <summary>Read orders.</summary>
    public const string OrderView   = "Order.View";

    /// <summary>Record payment on an order.</summary>
    public const string OrderPay = "Order.Pay";

    /// <summary>Ship an order.</summary>
    public const string OrderShip   = "Order.Ship";

    /// <summary>Cancel an order.</summary>
    public const string OrderCancel = "Order.Cancel";

    /// <summary>Read inventory.</summary>
    public const string InventoryView = "Inventory.View";
}
