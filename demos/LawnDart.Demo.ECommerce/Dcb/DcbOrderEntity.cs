using LawnDart;
using LawnDart.Dcb;
using LawnDart.Demo.ECommerce.Domain.Order;
using LawnDart.Demo.ECommerce.Domain.Order.Commands;
using LawnDart.Demo.ECommerce.Domain.Order.Events;

namespace LawnDart.Demo.ECommerce.Dcb;

/// <summary>
/// DCB-style Order Entity using the DcbEntity base class.
/// Demonstrates how to use the new DCB patterns with proper encapsulation.
/// </summary>
public class DcbOrderEntity : DcbEntity<OrderState>
{
    public void Handle(CreateOrderCommand command) => HandleCreateOrder(command);
    public void Handle(AddOrderItemCommand command) => HandleAddOrderItem(command);
    public void Handle(CompleteOrderCommand command) => HandleCompleteOrder(command);

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            case OrderCreated created:
                State.OrderId = created.OrderId;
                State.CartId = created.CartId;
                State.CustomerId = created.CustomerId;
                State.Items = new List<OrderItem>();
                State.IsCompleted = false;
                State.Total = 0m;
                break;
            case OrderItemAdded itemAdded:
                State.Items.Add(new OrderItem
                {
                    ProductId = itemAdded.ProductId,
                    Quantity = itemAdded.Quantity,
                    UnitPrice = itemAdded.UnitPrice
                });
                State.Total += itemAdded.Quantity * itemAdded.UnitPrice;
                break;
            case OrderCompleted:
                State.IsCompleted = true;
                break;
        }
    }

    private void HandleCreateOrder(CreateOrderCommand command)
    {
        if (State.OrderId != Guid.Empty)
        {
            throw new InvalidOperationException("Order already created");
        }

        var @event = new OrderCreated(
            Guid.NewGuid(),
            DateTime.UtcNow,
            command.OrderId,
            command.CartId,
            command.CustomerId);

        // Emit event with relevant tags
        Emit(@event,
            $"order:{command.OrderId}",
            $"cart:{command.CartId}",
            $"customer:{command.CustomerId}");
    }

    private void HandleAddOrderItem(AddOrderItemCommand command)
    {
        if (State.OrderId == Guid.Empty)
        {
            throw new InvalidOperationException("Order must be created before adding items");
        }

        if (State.IsCompleted)
        {
            throw new InvalidOperationException("Cannot add items to completed order");
        }

        var @event = new OrderItemAdded(
            Guid.NewGuid(),
            DateTime.UtcNow,
            command.ProductId,
            command.Quantity,
            command.UnitPrice);

        // Emit with order and product tags
        Emit(@event,
            $"order:{State.OrderId}",
            $"product:{command.ProductId}");
    }

    private void HandleCompleteOrder(CompleteOrderCommand command)
    {
        if (State.OrderId == Guid.Empty)
        {
            throw new InvalidOperationException("Order must be created before completing");
        }

        if (State.IsCompleted)
        {
            throw new InvalidOperationException("Order already completed");
        }

        if (State.Items.Count == 0)
        {
            throw new InvalidOperationException("Cannot complete order with no items");
        }

        var @event = new OrderCompleted(Guid.NewGuid(), DateTime.UtcNow);

        Emit(@event, $"order:{State.OrderId}");
    }
}
