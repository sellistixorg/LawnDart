using LawnDart;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Product.Commands;

public record UpdateInventoryCommand(
    [PropertyOrder(1)] Guid Id, 
    [PropertyOrder(2)] int QuantityDelta) : ICommand;


