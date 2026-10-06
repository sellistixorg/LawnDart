using LawnDart;
using LawnDart.Authorization;
using LawnDart.Demo.Shop.Authorization;

namespace LawnDart.Demo.Shop.Domain.Order.Commands;

[RequiresPermission(ShopPermissions.OrderPlace)]
public record PlaceOrderCommand(
    Guid Id,
    Guid OrderId,
    Guid ProductId,
    string ProductName,
    int Quantity,
    decimal UnitPrice,
    string SellerId          = "",
    string SellerName        = "",
    Guid SellerFulfillmentId = default,
    string CustomerId        = "",
    string CustomerName      = "",
    string TenantId          = "") : ICommand;

public record ProcessPaymentCommand(
    Guid Id,
    Guid OrderId,
    string PaymentReference) : ICommand;

[RequiresPermission(ShopPermissions.OrderShip)]
public record ShipOrderCommand(
    Guid Id,
    Guid OrderId,
    string TrackingNumber) : ICommand;

[RequiresPermission(ShopPermissions.OrderCancel)]
public record CancelOrderCommand(
    Guid Id,
    Guid OrderId,
    string Reason,
    string TenantId = "") : ICommand;
