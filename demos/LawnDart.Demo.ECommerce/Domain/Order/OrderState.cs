using LawnDart;

namespace LawnDart.Demo.ECommerce.Domain.Order;

public class OrderState : IState
{
    public Guid OrderId { get; set; }
    public Guid CartId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public List<OrderItem> Items { get; set; } = new();
    public bool IsCompleted { get; set; }
    public decimal Total { get; set; }
}

public class OrderItem
{
    public string ProductId { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal LineTotal => Quantity * UnitPrice;
}


