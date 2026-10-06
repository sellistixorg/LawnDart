using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Cart.Events;

[EventTypeName("cart-checked-out")]
public partial record CartCheckedOut(
    [property: PropertyOrder(1)] Guid Id,
    [property: PropertyOrder(2)] DateTime Timestamp,
    [property: PropertyOrder(3)] Guid OrderId) : IEvent;
