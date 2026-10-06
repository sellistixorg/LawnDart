using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Cart.Events;

[EventTypeName("cart-created")]
public partial record CartCreated(
    [property: PropertyOrder(1)] Guid Id,
    [property: PropertyOrder(2)] DateTime Timestamp,
    [property: PropertyOrder(3)] Guid CartId,
    [property: PropertyOrder(4)] string CustomerId) : IEvent;
