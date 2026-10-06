using LawnDart;
using LawnDart.Authorization;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Order.Commands;

[RequiresPermission("Orders.Create")]
public record CreateOrderCommand(
    [PropertyOrder(1)] Guid Id, 
    [PropertyOrder(2)] Guid OrderId, 
    [PropertyOrder(3)] Guid CartId, 
    [PropertyOrder(4)] string CustomerId) : ICommand;


