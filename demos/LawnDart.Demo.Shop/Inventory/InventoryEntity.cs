using LawnDart;
using LawnDart.Dcb;
using LawnDart.Demo.Shop.Domain.Order.Commands;
using LawnDart.Demo.Shop.Domain.Product.Events;
namespace LawnDart.Demo.Shop.Inventory;

/// <summary>
/// DCB entity that reserves stock for one product. The reservation is its own append.
/// The order aggregate is a second append. Two buys of the last unit conflict on this
/// reservation: one commit wins, and the other is rejected (409 or 422).
/// </summary>
public class InventoryEntity : DcbEntity<InventoryState>
{
    public InventoryEntity() { }

    public static string[] GetTags(Guid productId, Guid orderId)
        => [$"product:{productId}", $"order:{orderId}"];

    public static string[] GetProductTags(Guid productId)
        => [$"product:{productId}"];

    public void Handle(PlaceOrderCommand cmd)
    {
        if (State.Stock < cmd.Quantity)
            throw new DomainException(
                $"Insufficient stock for product '{State.ProductName}'. " +
                $"Available: {State.Stock}, Requested: {cmd.Quantity}.");

        // Emit StockReserved tagged product-only so the DCB consistency boundary spans
        // all concurrent buyers for the same product (AND-semantics load query).
        Emit(new StockReserved(Guid.NewGuid(), DateTime.UtcNow, cmd.ProductId, cmd.OrderId, cmd.Quantity),
            $"product:{cmd.ProductId}");
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            // InventoryAggregate emits this when a product is first created
            case InventoryInitialized e:
                State.ProductId   = e.ProductId;
                State.ProductName = string.Empty; // name lives on ProductAggregate
                State.Stock       = e.InitialQuantity;
                break;

            case StockUpdated e:
                // Use delta, not e.NewStock : the InventoryAggregate that emitted
                // this event only sees its own stream and doesn't know about DCB
                // StockReserved events. Applying delta against the running total
                // here (which already includes all reservations) gives the correct value.
                State.Stock = Math.Max(0, State.Stock + e.Delta);
                break;

            case StockReserved e when e.ProductId == State.ProductId:
                State.Stock = Math.Max(0, State.Stock - e.Quantity);
                break;
        }
    }
}
