using LawnDart;

namespace LawnDart.Demo.ECommerce.Projections;

public class OrderView : IState
{
    public Guid OrderId { get; set; }
    public Guid CartId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public List<OrderItemView> Items { get; set; } = new();
    public decimal Total { get; set; }
    public bool IsCompleted { get; set; }
}

public class OrderItemView
{
    public string ProductId { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal LineTotal { get; set; }
}


