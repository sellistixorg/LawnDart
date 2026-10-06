using LawnDart;
using LawnDart.Demo.Shop.Domain.Order.Commands;
using LawnDart.Demo.Shop.Domain.Order.Events;
using LawnDart.Demo.Shop.Infrastructure;
using LawnDart.Messaging;
using LawnDart.Patterns.Reaction;

namespace LawnDart.Demo.Shop.EDA;

/// <summary>
/// Reacts to OrderPlaced by issuing ProcessPaymentCommand.
/// Demonstrates choreography-based EDA: the OrderReactor connects the first
/// domain event to the next step in the business process automatically.
/// </summary>
public sealed class OrderReactor : IReactor<OrderPlaced>
{
    public Task<IEnumerable<ICommand>> ReactAsync(
        OrderPlaced @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        IEnumerable<ICommand> commands =
        [
            new ProcessPaymentCommand(
                Guid.NewGuid(),
                @event.OrderId,
                $"PAY-{@event.OrderId:N}"[..12])
        ];
        return Task.FromResult(commands);
    }
}

/// <summary>
/// Reacts to OrderPaymentProcessed by issuing ShipOrderCommand.
/// Completes the EDA chain: Place -> Pay -> Ship, all driven by events.
/// </summary>
public sealed class FulfillmentReactor : IReactor<OrderPaymentProcessed>
{
    public Task<IEnumerable<ICommand>> ReactAsync(
        OrderPaymentProcessed @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        IEnumerable<ICommand> commands =
        [
            new ShipOrderCommand(
                Guid.NewGuid(),
                @event.OrderId,
                $"TRACK-{@event.OrderId:N}"[..14])
        ];
        return Task.FromResult(commands);
    }
}
