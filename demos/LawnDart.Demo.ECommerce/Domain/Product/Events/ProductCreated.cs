using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Product.Events;

[EventTypeName("product-created")]
public partial record ProductCreated(
    [property: PropertyOrder(1)] Guid Id,
    [property: PropertyOrder(2)] DateTime Timestamp,
    [property: PropertyOrder(3)] Guid ProductId,
    [property: PropertyOrder(4)] string Name,
    [property: PropertyOrder(5)] string Sku,
    [property: PropertyOrder(6)] decimal Price,
    [property: PropertyOrder(7)] int InitialInventory) : IEvent;
