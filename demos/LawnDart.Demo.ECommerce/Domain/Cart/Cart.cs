using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Demo.ECommerce.Domain.Cart.Commands;
using LawnDart.Demo.ECommerce.Domain.Cart.Events;

namespace LawnDart.Demo.ECommerce.Domain.Cart;

public class Cart : AggregateRoot<CartState>
{
    public Cart()
    {
        State = new CartState();
    }

    public void Handle(CreateCartCommand command)
    {
        Apply(new CartCreated(
            Guid.NewGuid(),
            DateTime.UtcNow,
            command.CartId,
            command.CustomerId));
    }

    public void Handle(AddItemToCartCommand command)
    {
        if (State.IsCheckedOut)
        {
            throw new InvalidOperationException("Cannot add items to a checked out cart");
        }

        if (command.Quantity <= 0)
        {
            throw new ArgumentException("Quantity must be greater than zero", nameof(command));
        }

        Apply(new ItemAddedToCart(
            Guid.NewGuid(),
            DateTime.UtcNow,
            command.ProductId,
            command.Quantity));
    }

    public void Handle(RemoveItemFromCartCommand command)
    {
        if (State.IsCheckedOut)
        {
            throw new InvalidOperationException("Cannot remove items from a checked out cart");
        }

        var existingItem = State.Items.FirstOrDefault(i => i.ProductId == command.ProductId);
        if (existingItem == null)
        {
            throw new InvalidOperationException($"Product {command.ProductId} is not in the cart");
        }

        Apply(new ItemRemovedFromCart(
            Guid.NewGuid(),
            DateTime.UtcNow,
            command.ProductId));
    }

    public void Handle(CheckoutCartCommand command)
    {
        if (State.IsCheckedOut)
        {
            throw new InvalidOperationException("Cart is already checked out");
        }

        if (State.Items.Count == 0)
        {
            throw new InvalidOperationException("Cannot checkout an empty cart");
        }

        var orderId = Guid.NewGuid();
        Apply(new CartCheckedOut(
            Guid.NewGuid(),
            DateTime.UtcNow,
            orderId));
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            case CartCreated created:
                State.CartId = created.CartId;
                State.CustomerId = created.CustomerId;
                State.Items = new List<CartItem>();
                State.IsCheckedOut = false;
                break;

            case ItemAddedToCart itemAdded:
                var existingItem = State.Items.FirstOrDefault(i => i.ProductId == itemAdded.ProductId);
                if (existingItem != null)
                {
                    existingItem.Quantity += itemAdded.Quantity;
                }
                else
                {
                    State.Items.Add(new CartItem
                    {
                        ProductId = itemAdded.ProductId,
                        Quantity = itemAdded.Quantity
                    });
                }
                break;

            case ItemRemovedFromCart itemRemoved:
                var itemToRemove = State.Items.FirstOrDefault(i => i.ProductId == itemRemoved.ProductId);
                if (itemToRemove != null)
                {
                    State.Items.Remove(itemToRemove);
                }
                break;

            case CartCheckedOut checkedOut:
                State.IsCheckedOut = true;
                State.OrderId = checkedOut.OrderId;
                break;
        }
    }
}


