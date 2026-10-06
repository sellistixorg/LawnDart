using LawnDart;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Product.Commands;

public record UpdatePriceCommand(
    [PropertyOrder(1)] Guid Id, 
    [PropertyOrder(2)] decimal NewPrice) : ICommand;


