using LawnDart.Messaging;
using LawnDart.Patterns.EventProcessing;
using LawnDart.Patterns.Reaction;
using LawnDart.Patterns.TaskProcessing;

namespace LawnDart.Messaging.Tests;

// ── Events ──────────────────────────────────────────────────────────────────

internal record OrderPlacedEvent(Guid Id, DateTime Timestamp, string OrderId) : IEvent;
internal record InventoryReservedEvent(Guid Id, DateTime Timestamp, string OrderId) : IEvent;

// ── Commands ─────────────────────────────────────────────────────────────────

internal record SendNotificationCommand(Guid Id, string OrderId) : ICommand;
internal record CancelOverdueOrderCommand(Guid Id, string OrderId) : ICommand;

// ── Reactor ───────────────────────────────────────────────────────────────────

/// <summary>
/// Emits a <see cref="SendNotificationCommand"/> for every <see cref="OrderPlacedEvent"/>.
/// </summary>
internal sealed class OrderPlacedReactor : IReactor<OrderPlacedEvent>
{
    public int CallCount { get; private set; }

    public Task<IEnumerable<ICommand>> ReactAsync(
        OrderPlacedEvent @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        CallCount++;
        return Task.FromResult<IEnumerable<ICommand>>(
            [new SendNotificationCommand(Guid.NewGuid(), @event.OrderId)]);
    }
}

/// <summary>
/// Reactor that always throws, for error-path testing.
/// </summary>
internal sealed class ThrowingReactor : IReactor<OrderPlacedEvent>
{
    public Task<IEnumerable<ICommand>> ReactAsync(
        OrderPlacedEvent @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("Reactor failure");
}

// ── Event processor ───────────────────────────────────────────────────────────

/// <summary>
/// Transforms <see cref="OrderPlacedEvent"/> into <see cref="InventoryReservedEvent"/>.
/// </summary>
internal sealed class InventoryEventProcessor : IEventProcessor<OrderPlacedEvent>
{
    public int CallCount { get; private set; }

    public Task<IEnumerable<IEvent>> ProcessAsync(
        OrderPlacedEvent @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        CallCount++;
        return Task.FromResult<IEnumerable<IEvent>>(
            [new InventoryReservedEvent(Guid.NewGuid(), DateTime.UtcNow, @event.OrderId)]);
    }
}

// ── Task processor ────────────────────────────────────────────────────────────

/// <summary>
/// Emits a <see cref="CancelOverdueOrderCommand"/> on each poll when there are overdue orders.
/// </summary>
internal sealed class OverdueOrderTaskProcessor : ITaskProcessor
{
    private readonly List<string> _overdueOrderIds;
    public int CallCount { get; private set; }

    public OverdueOrderTaskProcessor(params string[] overdueOrderIds)
    {
        _overdueOrderIds = [.. overdueOrderIds];
    }

    public Task<IEnumerable<ICommand>> ProcessTasksAsync(CancellationToken cancellationToken = default)
    {
        CallCount++;
        return Task.FromResult<IEnumerable<ICommand>>(
            _overdueOrderIds.Select(id => (ICommand)new CancelOverdueOrderCommand(Guid.NewGuid(), id)));
    }
}

/// <summary>
/// Task processor that emits no commands.
/// </summary>
internal sealed class EmptyTaskProcessor : ITaskProcessor
{
    public Task<IEnumerable<ICommand>> ProcessTasksAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<IEnumerable<ICommand>>([]);
}

// ── Command dispatcher ────────────────────────────────────────────────────────

/// <summary>
/// Captures all dispatched commands for assertion in tests.
/// </summary>
internal sealed class CapturingCommandDispatcher : ICommandDispatcher
{
    public List<ICommand> Dispatched { get; } = [];

    public Task DispatchAsync(ICommand command, MessageContext context, CancellationToken cancellationToken = default)
    {
        Dispatched.Add(command);
        return Task.CompletedTask;
    }
}
