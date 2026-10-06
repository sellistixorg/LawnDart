using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Demo.Shop.Domain.Product.Commands;
using LawnDart.Demo.Shop.Domain.Product.Events;

namespace LawnDart.Demo.Shop.Domain.Product;

/// <summary>
/// Owns the stock level for a single product.
/// Separated from <see cref="ProductAggregate"/> because inventory lifecycle
/// (replenishment, reservations, write-offs) is a distinct bounded context
/// from product catalogue management (name, description, price).
///
/// Stream: InventoryAggregate:{productId}
/// </summary>
public class InventoryAggregate : AggregateRoot<InventoryAggregateState>
{
    public InventoryAggregate()
    {
        State = new InventoryAggregateState();
    }

    public void Handle(InitializeInventoryCommand cmd)
    {
        if (State.Initialized)
            throw new DomainException($"Inventory for product {cmd.ProductId} is already initialized.");

        Apply(new InventoryInitialized(Guid.NewGuid(), DateTime.UtcNow,
            cmd.ProductId, cmd.InitialQuantity));
    }

    public void Handle(UpdateStockCommand cmd)
    {
        if (!State.Initialized)
            throw new DomainException($"Inventory for product {cmd.ProductId} has not been initialized.");

        var newStock = State.Stock + cmd.Delta;
        if (newStock < 0)
            throw new DomainException(
                $"Insufficient stock. Current: {State.Stock}, Delta: {cmd.Delta}");

        Apply(new StockUpdated(Guid.NewGuid(), DateTime.UtcNow, cmd.ProductId, newStock, cmd.Delta));
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            case InventoryInitialized e:
                State.ProductId   = e.ProductId;
                State.Stock       = e.InitialQuantity;
                State.Initialized = true;
                break;

            case StockUpdated e:
                State.Stock = e.NewStock;
                break;
        }
    }
}
