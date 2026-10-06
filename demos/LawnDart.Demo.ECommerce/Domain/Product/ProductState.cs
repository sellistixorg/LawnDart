using LawnDart;

namespace LawnDart.Demo.ECommerce.Domain.Product;

public class ProductState : IState
{
    public Guid ProductId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Sku { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Inventory { get; set; }
}


