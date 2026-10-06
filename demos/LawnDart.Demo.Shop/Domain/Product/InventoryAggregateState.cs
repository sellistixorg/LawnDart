using LawnDart;

namespace LawnDart.Demo.Shop.Domain.Product;

public class InventoryAggregateState : IState
{
    public Guid ProductId { get; set; }
    public int Stock { get; set; }
    public bool Initialized { get; set; }
}
