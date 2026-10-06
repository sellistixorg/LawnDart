using LawnDart;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Order.Commands;

public record CompleteOrderCommand(
    [PropertyOrder(1)] Guid Id) : ICommand;


