using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Demo.Shop.Domain.Product.Commands;
using LawnDart.Demo.Shop.Domain.Product.Events;

namespace LawnDart.Demo.Shop.Domain.Product;

/// <summary>
/// Owns product catalogue data: name, description, price.
/// Stock levels are the responsibility of <see cref="InventoryAggregate"/>.
/// </summary>
public class ProductAggregate : AggregateRoot<ProductState>
{
    public ProductAggregate()
    {
        State = new ProductState();
    }

    public void Handle(CreateProductCommand cmd)
    {
        if (State.Exists)
            throw new DomainException($"Product {cmd.ProductId} already exists.");

        Apply(new ProductCreated(Guid.NewGuid(), DateTime.UtcNow,
            cmd.ProductId, cmd.Name, cmd.Description, cmd.Price, cmd.InitialStock, cmd.Sku,
            cmd.SellerId, cmd.SellerName));
    }

    public void Handle(UpdatePriceCommand cmd)
    {
        if (!State.Exists)
            throw new DomainException($"Product {cmd.ProductId} does not exist.");
        if (cmd.NewPrice <= 0)
            throw new DomainException("Price must be greater than zero.");

        Apply(new PriceUpdated(Guid.NewGuid(), DateTime.UtcNow, cmd.ProductId, cmd.NewPrice));
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            case ProductCreated e:
                State.ProductId   = e.ProductId;
                State.Name        = e.Name;
                State.Description = e.Description;
                State.Price       = e.Price;
                State.Sku         = e.Sku;
                State.SellerId    = e.SellerId;
                State.SellerName  = e.SellerName;
                State.Exists      = true;
                break;

            case PriceUpdated e:
                State.Price = e.NewPrice;
                break;
        }
    }
}
