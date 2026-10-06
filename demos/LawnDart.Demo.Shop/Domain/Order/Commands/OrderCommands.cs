using LawnDart;
using LawnDart.Authorization;
using LawnDart.Demo.Shop.Authorization;

namespace LawnDart.Demo.Shop.Domain.Order.Commands;

/// <summary>
/// Places an order for one product. Stock reservation and the order append are separate writes.
/// </summary>
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

/// <summary>
/// Records payment for an order. Callers need <see cref="ShopPermissions.OrderPay"/>.
/// </summary>
[RequiresPermission(ShopPermissions.OrderPay)]
public record ProcessPaymentCommand(
    Guid Id,
    Guid OrderId,
    string PaymentReference) : ICommand;

/// <summary>
/// Ships an order. <paramref name="TenantId"/> is the buyer tenant on the order stream.
/// </summary>
[RequiresPermission(ShopPermissions.OrderShip)]
public record ShipOrderCommand(
    Guid Id,
    Guid OrderId,
    string TrackingNumber,
    string TenantId = "") : ICommand;

/// <summary>
/// Cancels an order. <paramref name="TenantId"/> is the buyer tenant on the order stream.
/// </summary>
[RequiresPermission(ShopPermissions.OrderCancel)]
public record CancelOrderCommand(
    Guid Id,
    Guid OrderId,
    string Reason,
    string TenantId = "") : ICommand;
