using LawnDart;

namespace LawnDart.Demo.Shop.Inventory;

public class InventoryState : IState
{
    public Guid ProductId { get; set; }
    public int Stock { get; set; }
    public string ProductName { get; set; } = string.Empty;
}
