using LawnDart;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Cart.Commands;

public record AddItemToCartCommand(
    [PropertyOrder(1)] Guid Id, 
    [PropertyOrder(2)] string ProductId, 
    [PropertyOrder(3)] int Quantity) : ICommand;


