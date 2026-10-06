using LawnDart;

namespace LawnDart.Demo.ECommerce.Projections;

public class CartView : IState
{
    public Guid CartId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public List<CartItemView> Items { get; set; } = new();
    public decimal Total { get; set; }
    public int ItemCount { get; set; }
    public bool IsCheckedOut { get; set; }
    public Guid? OrderId { get; set; }
}

public class CartItemView
{
    public string ProductId { get; set; } = string.Empty;
    public int Quantity { get; set; }
}


