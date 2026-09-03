// Fixtures placed in a realistic nested namespace to test group derivation by namespace.
using LawnDart;

namespace LawnDart.AspNetCore.Tests.Namespaced.Orders.Commands
{
    public record CreateOrderCommand(Guid Id) : ICommand;
}
