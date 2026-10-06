using LawnDart;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Cart.Commands;

public record CreateCartCommand(
    [PropertyOrder(1)] Guid Id, 
    [PropertyOrder(2)] Guid CartId, 
    [PropertyOrder(3)] string CustomerId) : ICommand;


