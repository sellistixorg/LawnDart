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
/// <remarks>
/// Polls run on the hosted service's background task, so <see cref="CallCount"/> is written from a
/// different thread than the one asserting on it. <see cref="FirstPollCompleted"/> lets a test wait
/// for a poll to actually happen instead of sleeping and hoping.
/// </remarks>
internal sealed class OverdueOrderTaskProcessor : ITaskProcessor
{
    private readonly List<string> _overdueOrderIds;
    private readonly TaskCompletionSource _firstPoll = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _callCount;

    public int CallCount => Volatile.Read(ref _callCount);

    /// <summary>Completes once <see cref="ProcessTasksAsync"/> has run at least once.</summary>
    public Task FirstPollCompleted => _firstPoll.Task;

    public OverdueOrderTaskProcessor(params string[] overdueOrderIds)
    {
        _overdueOrderIds = [.. overdueOrderIds];
    }

    public Task<IEnumerable<ICommand>> ProcessTasksAsync(CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _callCount);

        var commands = _overdueOrderIds
            .Select(id => (ICommand)new CancelOverdueOrderCommand(Guid.NewGuid(), id))
            .ToList();

        _firstPoll.TrySetResult();
        return Task.FromResult<IEnumerable<ICommand>>(commands);
    }
}

/// <summary>
/// Task processor that emits no commands.
/// </summary>
internal sealed class EmptyTaskProcessor : ITaskProcessor
{
    private readonly TaskCompletionSource _firstPoll = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _callCount;

    public int CallCount => Volatile.Read(ref _callCount);

    /// <summary>Completes once <see cref="ProcessTasksAsync"/> has run at least once.</summary>
    public Task FirstPollCompleted => _firstPoll.Task;

    public Task<IEnumerable<ICommand>> ProcessTasksAsync(CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _callCount);
        _firstPoll.TrySetResult();
        return Task.FromResult<IEnumerable<ICommand>>([]);
    }
}

// ── Command dispatcher ────────────────────────────────────────────────────────

/// <summary>
/// Captures all dispatched commands for assertion in tests.
/// </summary>
/// <remarks>
/// Dispatches may arrive on a hosted service's background task, so the backing list is guarded and
/// <see cref="Dispatched"/> returns a snapshot. Use <see cref="WaitForDispatchAsync"/> to wait for an
/// expected number of commands rather than sleeping.
/// </remarks>
internal sealed class CapturingCommandDispatcher : ICommandDispatcher
{
    private readonly List<ICommand> _dispatched = [];
    private readonly SemaphoreSlim _dispatchSignal = new(0);

    public IReadOnlyList<ICommand> Dispatched
    {
        get { lock (_dispatched) { return [.. _dispatched]; } }
    }

    public Task DispatchAsync(ICommand command, MessageContext context, CancellationToken cancellationToken = default)
    {
        lock (_dispatched) { _dispatched.Add(command); }
        _dispatchSignal.Release();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Waits until <paramref name="count"/> commands have been dispatched, or throws on timeout.
    /// </summary>
    public async Task WaitForDispatchAsync(int count, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        for (var i = 0; i < count; i++)
        {
            try
            {
                await _dispatchSignal.WaitAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException(
                    $"Expected {count} dispatched command(s) within {timeout}, but only {Dispatched.Count} arrived.");
            }
        }
    }
}
