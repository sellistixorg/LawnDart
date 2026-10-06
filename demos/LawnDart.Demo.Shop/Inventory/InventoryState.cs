using LawnDart;

namespace LawnDart.Demo.Shop.Inventory;

/// <summary>
/// Stock seen by <see cref="InventoryEntity"/> across product and reservation events.
/// </summary>
public class InventoryState : IState
{
    public Guid ProductId { get; set; }
    public int Stock { get; set; }
    public string ProductName { get; set; } = string.Empty;
}
