using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Order.Events;

[EventTypeName("order-completed")]
public partial record OrderCompleted(
    [property: PropertyOrder(1)] Guid Id,
    [property: PropertyOrder(2)] DateTime Timestamp) : IEvent;
