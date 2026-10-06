using LawnDart;
using LawnDart.EventStore;

namespace LawnDart.Demo.Shop.Domain.Order.Events;

[EventTypeName("order-placed")]
public record OrderPlaced(
    Guid Id,
    DateTime Timestamp,
    Guid OrderId,
    string CustomerId,
    string CustomerName,
    Guid ProductId,
    string ProductName,
    int Quantity,
    decimal UnitPrice,
    string SellerId,
    string SellerName,
    Guid SellerFulfillmentId,
    string TenantId) : IEvent;

[EventTypeName("order-payment-processed")]
public record OrderPaymentProcessed( Guid Id, DateTime Timestamp, Guid OrderId, string PaymentReference) : IEvent;

[EventTypeName("order-shipped")]
public record OrderShipped( Guid Id, DateTime Timestamp, Guid OrderId, string TrackingNumber) : IEvent;

[EventTypeName("order-cancelled")]
public record OrderCancelled( Guid Id, DateTime Timestamp, Guid OrderId, string Reason) : IEvent;
