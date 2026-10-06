using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Cart.Events;

[EventTypeName("item-added-to-cart")]
public partial record ItemAddedToCart(
    [property: PropertyOrder(1)] Guid Id,
    [property: PropertyOrder(2)] DateTime Timestamp,
    [property: PropertyOrder(3)] string ProductId,
    [property: PropertyOrder(4)] int Quantity) : IEvent;
