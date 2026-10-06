using LawnDart;

namespace LawnDart.Demo.Shop.Domain.Product;

/// <summary>
/// On-hand stock for one product, before DCB reservations.
/// </summary>
public class InventoryAggregateState : IState
{
    public Guid ProductId { get; set; }
    public int Stock { get; set; }
    public bool Initialized { get; set; }
}
