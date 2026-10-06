using LawnDart;

namespace LawnDart.Demo.Shop.Domain.Order;

public enum OrderStatus { Pending, PaymentProcessed, Shipped, Cancelled }

public class OrderState : IState
{
    public Guid OrderId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string CustomerName { get; set; } = string.Empty;
    public Guid ProductId { get; set; }
    public string ProductName { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public string SellerId { get; set; } = string.Empty;
    public string SellerName { get; set; } = string.Empty;
    public Guid SellerFulfillmentId { get; set; }
    public OrderStatus Status { get; set; } = OrderStatus.Pending;
    public DateTime PlacedAt { get; set; }
    public bool Exists { get; set; }
}
