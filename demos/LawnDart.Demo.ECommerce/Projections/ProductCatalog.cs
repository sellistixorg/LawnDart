using LawnDart;

namespace LawnDart.Demo.ECommerce.Projections;

public class ProductCatalog : IState
{
    public Dictionary<string, ProductView> Products { get; set; } = new();
}

public class ProductView
{
    public Guid ProductId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Sku { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Inventory { get; set; }
}


