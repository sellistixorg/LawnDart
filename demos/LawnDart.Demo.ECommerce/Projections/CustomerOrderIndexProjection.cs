using LawnDart;
using LawnDart.Demo.ECommerce.Domain.Order.Events;

namespace LawnDart.Demo.ECommerce.Projections;

/// <summary>
/// Global projection example - aggregates orders across all customers.
/// Runs on single node (node 0) only for cross-stream analysis.
/// 
/// Use case: Admin dashboard showing top customers, revenue analytics, etc.
/// This cannot be partitioned because it needs to see all orders across all streams.
/// </summary>
public class CustomerOrderIndexProjection
{
    /// <summary>
    /// Aggregates orders by customer for cross-customer analytics
    /// </summary>
    public Dictionary<string, CustomerOrderSummary> OrdersByCustomer { get; set; } = new();
    
    /// <summary>
    /// Global metrics across all customers
    /// </summary>
    public GlobalOrderMetrics GlobalMetrics { get; set; } = new();

    /// <summary>
    /// Required by projection system: Processes any event and updates the view
    /// </summary>
    public void ProcessEvent(IEvent evt, long sequencePosition)
    {
        switch (evt)
        {
            case OrderCreated created:
                HandleOrderCreated(created);
                break;
                
            case OrderItemAdded itemAdded:
                HandleOrderItemAdded(itemAdded);
                break;
                
            case OrderCompleted completed:
                HandleOrderCompleted(completed);
                break;
        }
    }

    /// <summary>
    /// Required by projection system: Returns the current view state
    /// </summary>
    public object GetView() => this;

    private void HandleOrderCreated(OrderCreated evt)
    {
        // Track new order for customer
        if (!OrdersByCustomer.TryGetValue(evt.CustomerId, out var summary))
        {
            summary = new CustomerOrderSummary
            {
                CustomerId = evt.CustomerId,
                Orders = new List<OrderSummary>()
            };
            OrdersByCustomer[evt.CustomerId] = summary;
        }
        
        summary.Orders.Add(new OrderSummary
        {
            OrderId = evt.OrderId.ToString(),
            CreatedAt = evt.Timestamp,
            Total = 0m,
            IsCompleted = false
        });
        
        summary.TotalOrders++;
        
        // Update global metrics
        GlobalMetrics.TotalOrders++;
        GlobalMetrics.TotalCustomers = OrdersByCustomer.Count;
    }
    
    private void HandleOrderItemAdded(OrderItemAdded evt)
    {
        // Find the order across all customers and update its total
        // Note: In production, you'd want to track order->customer mapping
        // For demo purposes, we iterate (inefficient but demonstrates the concept)
        foreach (var customerOrders in OrdersByCustomer.Values)
        {
            var order = customerOrders.Orders.FirstOrDefault(o => o.OrderId == evt.Id.ToString());
            if (order != null)
            {
                order.Total += evt.Quantity * evt.UnitPrice;
                break;
            }
        }
    }
    
    private void HandleOrderCompleted(OrderCompleted evt)
    {
        // Mark order as completed and update revenue metrics
        foreach (var customerOrders in OrdersByCustomer.Values)
        {
            var order = customerOrders.Orders.FirstOrDefault(o => o.OrderId == evt.Id.ToString());
            if (order != null)
            {
                order.IsCompleted = true;
                order.CompletedAt = evt.Timestamp;
                
                // Update customer metrics
                customerOrders.TotalRevenue += order.Total;
                customerOrders.CompletedOrders++;
                
                // Update global metrics
                GlobalMetrics.TotalRevenue += order.Total;
                GlobalMetrics.CompletedOrders++;
                
                break;
            }
        }
    }
}

/// <summary>
/// Per-customer order summary
/// </summary>
public class CustomerOrderSummary
{
    public string CustomerId { get; set; } = string.Empty;
    public List<OrderSummary> Orders { get; set; } = new();
    public int TotalOrders { get; set; }
    public int CompletedOrders { get; set; }
    public decimal TotalRevenue { get; set; }
}

/// <summary>
/// Individual order summary
/// </summary>
public class OrderSummary
{
    public string OrderId { get; set; } = string.Empty;
    public decimal Total { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public bool IsCompleted { get; set; }
}

/// <summary>
/// Global metrics across all customers
/// </summary>
public class GlobalOrderMetrics
{
    public int TotalCustomers { get; set; }
    public int TotalOrders { get; set; }
    public int CompletedOrders { get; set; }
    public decimal TotalRevenue { get; set; }
}
