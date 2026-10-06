using LawnDart.Messaging;
using LawnDart.Patterns.Reaction;
using LawnDart.EventStore;

namespace LawnDart.Demo.ECommerce.EDA;

// -- EDA event & command types used across this demo --------------------------

/// <summary>Published when a customer places an order.</summary>
[EventTypeName("order-placed")]
public record OrderPlaced(
    Guid Id,
    DateTime Timestamp,
    string OrderId,
    string CustomerId,
    decimal Amount) : IEvent;

/// <summary>Published when an order is dispatched from the warehouse.</summary>
[EventTypeName("order-shipped")]
public record OrderShipped(
    Guid Id,
    DateTime Timestamp,
    string OrderId,
    string CustomerId,
    string TrackingNumber) : IEvent;

/// <summary>Derived event: emitted by <see cref="InventoryEventProcessor"/> when stock is reserved.</summary>
[EventTypeName("eda-inventory-reserved")]
public record InventoryReserved(
    Guid Id,
    DateTime Timestamp,
    string OrderId,
    int QuantityReserved) : IEvent;

/// <summary>Emitted when payment is successfully authorised for an order.</summary>
[EventTypeName("eda-payment-authorized")]
public record PaymentAuthorized(
    Guid Id,
    DateTime Timestamp,
    string OrderId,
    decimal Amount,
    string ShippingAddress) : IEvent;

/// <summary>Instructs the payment service to charge the customer.</summary>
public record ProcessPaymentCommand(
    Guid Id,
    string OrderId,
    decimal Amount) : ICommand;

/// <summary>Instructs the fulfilment service to ship the order.</summary>
public record ShipOrderCommand(
    Guid Id,
    string OrderId,
    string ShippingAddress) : ICommand;

/// <summary>Instructs the notification service to send a shipping confirmation.</summary>
public record SendNotificationCommand(
    Guid Id,
    string OrderId,
    string CustomerId,
    string Message) : ICommand;

/// <summary>Instructs the order service to cancel an order whose payment is overdue.</summary>
public record CancelOverdueOrderCommand(
    Guid Id,
    string OrderId,
    string Reason) : ICommand;

// -- Reactor implementation ----------------------------------------------------

/// <summary>
/// Reactor: <see cref="OrderShipped"/> -> <see cref="SendNotificationCommand"/>.
/// <para>
/// Triggered by the message broker when an order ships; emits a command to notify the customer.
/// </para>
/// </summary>
public class OrderReactor : IReactor<OrderShipped>
{
    public Task<IEnumerable<ICommand>> ReactAsync(
        OrderShipped @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        var message =
            $"Great news! Your order {@event.OrderId} has shipped. " +
            $"Track it with: {@event.TrackingNumber}";

        return Task.FromResult<IEnumerable<ICommand>>(
        [
            new SendNotificationCommand(Guid.NewGuid(), @event.OrderId, @event.CustomerId, message),
        ]);
    }
}
