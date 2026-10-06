using LawnDart.Messaging;
using LawnDart.Patterns.EventProcessing;

namespace LawnDart.Demo.ECommerce.EDA;

/// <summary>
/// Event processor: <see cref="OrderPlaced"/> -> <see cref="InventoryReserved"/>.
/// <para>
/// Triggered by the message broker when an order is placed; reserves stock and publishes
/// an <see cref="InventoryReserved"/> derived event so downstream consumers can react.
/// </para>
/// </summary>
public class InventoryEventProcessor : IEventProcessor<OrderPlaced>
{
    public Task<IEnumerable<IEvent>> ProcessAsync(
        OrderPlaced @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        // In a real system this would call a stock service; here we simulate reserving 1 unit.
        var reserved = new InventoryReserved(
            Id: Guid.NewGuid(),
            Timestamp: DateTime.UtcNow,
            OrderId: @event.OrderId,
            QuantityReserved: 1);

        return Task.FromResult<IEnumerable<IEvent>>([reserved]);
    }
}
