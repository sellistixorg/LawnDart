// Shared test fixtures — commands and handlers used by unit and integration tests.

using LawnDart;
using LawnDart.Authorization;
using LawnDart.EventStore;

namespace LawnDart.AspNetCore.Tests;

[RequiresPermission("Orders.Create")]
public record CreateOrderCommand(Guid Id, string ProductCode, int Quantity) : ICommand;

public record ShipOrderCommand(Guid Id, string TrackingNumber) : ICommand;

[RequiresPermission("Orders.Validate")]
public record RejectAuthorizedOrderCommand(Guid Id) : ICommand;

public record RejectShipmentCommand(Guid Id) : ICommand;

[RequiresPermission("Orders.Concurrent")]
public record ConcurrentAuthorizedOrderCommand(Guid Id) : ICommand;

public record ConcurrentShipmentCommand(Guid Id) : ICommand;

/// <summary>A command whose name has no noun — tests edge-case suffix stripping.</summary>
public record SubmitCommand(Guid Id) : ICommand;

public class CreateOrderCommandHandler : ICommandHandler<CreateOrderCommand>
{
    public static int CallCount;
    public Task HandleAsync(CreateOrderCommand command, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref CallCount);
        return Task.CompletedTask;
    }
}

public class ShipOrderCommandHandler : ICommandHandler<ShipOrderCommand>
{
    public Task HandleAsync(ShipOrderCommand command, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}

public class RejectAuthorizedOrderCommandHandler : ICommandHandler<RejectAuthorizedOrderCommand>
{
    public Task HandleAsync(RejectAuthorizedOrderCommand command, CancellationToken cancellationToken = default)
        => Task.FromException(new DomainException($"Order {command.Id} violates a business rule."));
}

public class RejectShipmentCommandHandler : ICommandHandler<RejectShipmentCommand>
{
    public Task HandleAsync(RejectShipmentCommand command, CancellationToken cancellationToken = default)
        => Task.FromException(new DomainException($"Shipment {command.Id} violates a business rule."));
}

public class ConcurrentAuthorizedOrderCommandHandler : ICommandHandler<ConcurrentAuthorizedOrderCommand>
{
    public Task HandleAsync(ConcurrentAuthorizedOrderCommand command, CancellationToken cancellationToken = default)
        => Task.FromException(new ConcurrencyException($"Order {command.Id} was updated by another request."));
}

public class ConcurrentShipmentCommandHandler : ICommandHandler<ConcurrentShipmentCommand>
{
    public Task HandleAsync(ConcurrentShipmentCommand command, CancellationToken cancellationToken = default)
        => Task.FromException(new ConcurrencyException($"Shipment {command.Id} was updated by another request."));
}

/// <summary>Abstract handler — must NOT be discovered by the scanner.</summary>
public abstract class AbstractHandler<TCommand> : ICommandHandler<TCommand> where TCommand : ICommand
{
    public abstract Task HandleAsync(TCommand command, CancellationToken cancellationToken = default);
}
