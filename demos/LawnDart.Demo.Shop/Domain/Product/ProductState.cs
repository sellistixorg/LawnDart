using LawnDart;

namespace LawnDart.Demo.Shop.Domain.Product;

/// <summary>Product catalogue information : name, description, price, sku.
/// Stock levels live in <see cref="InventoryAggregateState"/>.</summary>
public class ProductState : IState
{
    public Guid ProductId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public string Sku { get; set; } = string.Empty;
    public string SellerId { get; set; } = string.Empty;
    public string SellerName { get; set; } = string.Empty;
    public bool Exists { get; set; }
}
