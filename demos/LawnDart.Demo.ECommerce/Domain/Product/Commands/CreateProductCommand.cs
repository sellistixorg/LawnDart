using LawnDart;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Product.Commands;

public record CreateProductCommand(
    [PropertyOrder(1)] Guid Id, 
    [PropertyOrder(2)] Guid ProductId, 
    [PropertyOrder(3)] string Name, 
    [PropertyOrder(4)] string Sku, 
    [PropertyOrder(5)] decimal Price, 
    [PropertyOrder(6)] int InitialInventory) : ICommand;


