using LawnDart;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Order.Commands;

public record AddOrderItemCommand(
    [PropertyOrder(1)] Guid Id, 
    [PropertyOrder(2)] string ProductId, 
    [PropertyOrder(3)] int Quantity, 
    [PropertyOrder(4)] decimal UnitPrice) : ICommand;


