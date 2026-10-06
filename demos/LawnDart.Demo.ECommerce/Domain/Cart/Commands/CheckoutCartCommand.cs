using LawnDart;
using LawnDart.Authorization;
using LawnDart.Serialization;

namespace LawnDart.Demo.ECommerce.Domain.Cart.Commands;

[RequiresPermission("Carts.Checkout")]
[RequiresEntitlement("CheckoutFeature")]
public record CheckoutCartCommand(
    [PropertyOrder(1)] Guid Id) : ICommand;


