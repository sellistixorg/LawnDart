using LawnDart;

namespace LawnDart.Demo.ECommerce.Domain.Cart;

public class CartState : IState
{
    public Guid CartId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public List<CartItem> Items { get; set; } = new();
    public bool IsCheckedOut { get; set; }
    public Guid? OrderId { get; set; }
}

public class CartItem
{
    public string ProductId { get; set; } = string.Empty;
    public int Quantity { get; set; }
}


