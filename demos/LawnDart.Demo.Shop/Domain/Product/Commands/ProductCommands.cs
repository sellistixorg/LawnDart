using LawnDart;
using LawnDart.Authorization;
using LawnDart.Demo.Shop.Authorization;

namespace LawnDart.Demo.Shop.Domain.Product.Commands;

[RequiresPermission(ShopPermissions.ProductCreate)]
public record CreateProductCommand(
    Guid Id,
    Guid ProductId,
    string Name,
    string Description,
    decimal Price,
    int InitialStock,
    string Sku,
    string SellerId   = "",
    string SellerName = "") : ICommand;

/// <summary>
/// Issued by the create-product handler to reserve a SKU through <see cref="SkuRegistryEntity"/>.
/// </summary>
public record ReserveSkuCommand(
    Guid Id,
    string Sku,
    Guid ProductId,
    string TenantId) : ICommand;

/// <summary>
/// Issued by the create-product handler to open the <see cref="InventoryAggregate"/> stream.
/// </summary>
public record InitializeInventoryCommand(
    Guid Id,
    Guid ProductId,
    int InitialQuantity) : ICommand;

[RequiresPermission(ShopPermissions.ProductUpdate)]
public record UpdateStockCommand(
    Guid Id,
    Guid ProductId,
    int Delta) : ICommand;

[RequiresPermission(ShopPermissions.ProductUpdate)]
public record UpdatePriceCommand(
    Guid Id,
    Guid ProductId,
    decimal NewPrice) : ICommand;
