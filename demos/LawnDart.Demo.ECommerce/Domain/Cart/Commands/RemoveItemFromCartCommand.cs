using LawnDart;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Cart.Commands;

public record RemoveItemFromCartCommand(
    [PropertyOrder(1)] Guid Id, 
    [PropertyOrder(2)] string ProductId) : ICommand;


