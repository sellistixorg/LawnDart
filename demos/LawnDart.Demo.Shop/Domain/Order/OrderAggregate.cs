using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Demo.Shop.Domain.Order.Commands;
using LawnDart.Demo.Shop.Domain.Order.Events;

namespace LawnDart.Demo.Shop.Domain.Order;

/// <summary>
/// One order stream: place, pay, ship, or cancel.
/// </summary>
public class OrderAggregate : AggregateRoot<OrderState>
{
    public OrderAggregate()
    {
        State = new OrderState();
    }

    public void Handle(PlaceOrderCommand cmd)
    {
        if (State.Exists)
            throw new DomainException($"Order {cmd.OrderId} already exists.");

        Apply(new OrderPlaced(Guid.NewGuid(), DateTime.UtcNow,
            cmd.OrderId, cmd.CustomerId, cmd.CustomerName,
            cmd.ProductId, cmd.ProductName, cmd.Quantity, cmd.UnitPrice,
            cmd.SellerId, cmd.SellerName, cmd.SellerFulfillmentId, cmd.TenantId));
    }

    public void Handle(ProcessPaymentCommand cmd)
    {
        if (!State.Exists)
            throw new DomainException($"Order {cmd.OrderId} does not exist.");
        if (State.Status != OrderStatus.Pending)
            throw new DomainException($"Order {cmd.OrderId} is not in Pending state.");

        Apply(new OrderPaymentProcessed(Guid.NewGuid(), DateTime.UtcNow, cmd.OrderId, cmd.PaymentReference));
    }

    public void Handle(ShipOrderCommand cmd)
    {
        if (!State.Exists)
            throw new DomainException($"Order {cmd.OrderId} does not exist.");
        if (State.Status != OrderStatus.PaymentProcessed)
            throw new DomainException($"Order {cmd.OrderId} must have payment processed before shipping.");

        Apply(new OrderShipped(Guid.NewGuid(), DateTime.UtcNow, cmd.OrderId, cmd.TrackingNumber));
    }

    public void Handle(CancelOrderCommand cmd)
    {
        if (!State.Exists)
            throw new DomainException($"Order {cmd.OrderId} does not exist.");
        if (State.Status == OrderStatus.Shipped)
            throw new DomainException($"Order {cmd.OrderId} has already shipped and cannot be cancelled.");
        if (State.Status == OrderStatus.Cancelled)
            return;

        Apply(new OrderCancelled(Guid.NewGuid(), DateTime.UtcNow, cmd.OrderId, cmd.Reason));
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            case OrderPlaced e:
                State.OrderId             = e.OrderId;
                State.CustomerId          = e.CustomerId;
                State.CustomerName        = e.CustomerName;
                State.ProductId           = e.ProductId;
                State.ProductName         = e.ProductName;
                State.Quantity            = e.Quantity;
                State.UnitPrice           = e.UnitPrice;
                State.SellerId            = e.SellerId;
                State.SellerName          = e.SellerName;
                State.SellerFulfillmentId = e.SellerFulfillmentId;
                State.Status              = OrderStatus.Pending;
                State.PlacedAt            = e.Timestamp;
                State.Exists              = true;
                break;

            case OrderPaymentProcessed:
                State.Status = OrderStatus.PaymentProcessed;
                break;

            case OrderShipped:
                State.Status = OrderStatus.Shipped;
                break;

            case OrderCancelled:
                State.Status = OrderStatus.Cancelled;
                break;
        }
    }
}
