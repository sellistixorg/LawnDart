using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Cart.Events;

[EventTypeName("item-removed-from-cart")]
public partial record ItemRemovedFromCart(
    [property: PropertyOrder(1)] Guid Id,
    [property: PropertyOrder(2)] DateTime Timestamp,
    [property: PropertyOrder(3)] string ProductId) : IEvent;
