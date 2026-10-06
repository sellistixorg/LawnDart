using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Order.Events;

[EventTypeName("order-item-added")]
public partial record OrderItemAdded(
    [property: PropertyOrder(1)] Guid Id,
    [property: PropertyOrder(2)] DateTime Timestamp,
    [property: PropertyOrder(3)] string ProductId,
    [property: PropertyOrder(4)] int Quantity,
    [property: PropertyOrder(5)] decimal UnitPrice) : IEvent;
