using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Order.Events;

[EventTypeName("order-created")]
public partial record OrderCreated(
    [property: PropertyOrder(1)] Guid Id,
    [property: PropertyOrder(2)] DateTime Timestamp,
    [property: PropertyOrder(3)] Guid OrderId,
    [property: PropertyOrder(4)] Guid CartId,
    [property: PropertyOrder(5)] string CustomerId) : IEvent;
