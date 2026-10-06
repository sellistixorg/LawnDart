using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Demo.ECommerce.Domain.Order.Commands;
using LawnDart.Demo.ECommerce.Domain.Order.Events;

namespace LawnDart.Demo.ECommerce.Domain.Order;

public class Order : AggregateRoot<OrderState>
{
    public Order()
    {
        State = new OrderState();
    }

    public void Handle(CreateOrderCommand command)
    {
        Apply(new OrderCreated(
            Guid.NewGuid(),
            DateTime.UtcNow,
            command.OrderId,
            command.CartId,
            command.CustomerId));
    }

    public void Handle(AddOrderItemCommand command)
    {
        if (State.IsCompleted)
        {
            throw new InvalidOperationException("Cannot modify a completed order");
        }

        if (command.Quantity <= 0)
        {
            throw new ArgumentException("Quantity must be greater than zero", nameof(command));
        }

        if (command.UnitPrice < 0)
        {
            throw new ArgumentException("Unit price cannot be negative", nameof(command));
        }

        Apply(new OrderItemAdded(
            Guid.NewGuid(),
            DateTime.UtcNow,
            command.ProductId,
            command.Quantity,
            command.UnitPrice));
    }

    public void Handle(CompleteOrderCommand command)
    {
        if (State.IsCompleted)
        {
            throw new InvalidOperationException("Order is already completed");
        }

        if (State.Items.Count == 0)
        {
            throw new InvalidOperationException("Cannot complete an order with no items");
        }

        Apply(new OrderCompleted(
            Guid.NewGuid(),
            DateTime.UtcNow));
    }

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
}


