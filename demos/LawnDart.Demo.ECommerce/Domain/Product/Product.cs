using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Demo.ECommerce.Domain.Product.Commands;
using LawnDart.Demo.ECommerce.Domain.Product.Events;

namespace LawnDart.Demo.ECommerce.Domain.Product;

public class Product : AggregateRoot<ProductState>
{
    public Product()
    {
        State = new ProductState();
    }

    public void Handle(CreateProductCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.Name))
        {
            throw new ArgumentException("Product name cannot be empty", nameof(command));
        }

        if (string.IsNullOrWhiteSpace(command.Sku))
        {
            throw new ArgumentException("Product SKU cannot be empty", nameof(command));
        }

        if (command.Price < 0)
        {
            throw new ArgumentException("Product price cannot be negative", nameof(command));
        }

        if (command.InitialInventory < 0)
        {
            throw new ArgumentException("Initial inventory cannot be negative", nameof(command));
        }

        Apply(new ProductCreated(
            Guid.NewGuid(),
            DateTime.UtcNow,
            command.ProductId,
            command.Name,
            command.Sku,
            command.Price,
            command.InitialInventory));
    }

    public void Handle(UpdatePriceCommand command)
    {
        if (State.ProductId == Guid.Empty)
        {
            throw new InvalidOperationException("Product has not been created yet");
        }

        if (command.NewPrice < 0)
        {
            throw new ArgumentException("Price cannot be negative", nameof(command));
        }

        Apply(new PriceUpdated(
            Guid.NewGuid(),
            DateTime.UtcNow,
            command.NewPrice));
    }

    public void Handle(UpdateInventoryCommand command)
    {
        if (State.ProductId == Guid.Empty)
        {
            throw new InvalidOperationException("Product has not been created yet");
        }

        var newQuantity = State.Inventory + command.QuantityDelta;
        if (newQuantity < 0)
        {
            throw new InvalidOperationException($"Insufficient inventory. Current: {State.Inventory}, Requested change: {command.QuantityDelta}");
        }

        Apply(new InventoryUpdated(
            Guid.NewGuid(),
            DateTime.UtcNow,
            newQuantity,
            command.QuantityDelta));
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            case ProductCreated created:
                State.ProductId = created.ProductId;
                State.Name = created.Name;
                State.Sku = created.Sku;
                State.Price = created.Price;
                State.Inventory = created.InitialInventory;
                break;

            case PriceUpdated priceUpdated:
                State.Price = priceUpdated.NewPrice;
                break;

            case InventoryUpdated inventoryUpdated:
                State.Inventory = inventoryUpdated.NewQuantity;
                break;
        }
    }
}


