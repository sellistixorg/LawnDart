using LawnDart;
using LawnDart.EventStore;
using Microsoft.Extensions.Logging;

namespace LawnDart.Demo.MultiContextInMemory.Ordering;

/// <summary>Places an order on the ordering stream.</summary>
public sealed record PlaceOrderCommand(Guid Id, string CustomerId, IReadOnlyList<string> Items) : ICommand;

/// <summary>Marks an existing order fulfilled.</summary>
public sealed record FulfillOrderCommand(Guid Id) : ICommand;

/// <summary>An order was placed for a customer.</summary>
[EventTypeName("order-placed")]
public record OrderPlaced(Guid Id, DateTime Timestamp, string CustomerId, IReadOnlyList<string> Items) : IEvent;

/// <summary>An order was fulfilled.</summary>
[EventTypeName("order-fulfilled")]
public record OrderFulfilled(Guid Id, DateTime Timestamp) : IEvent;

/// <summary>
/// Handles <see cref="PlaceOrderCommand"/>.
/// Injects <see cref="IEventStore"/>. The ordering store is supplied by the
/// context that registered this handler. The handler does not name a context key.
/// </summary>
public sealed class PlaceOrderHandler : ICommandHandler<PlaceOrderCommand>
{
    private readonly IEventStore _eventStore;
    private readonly ILogger<PlaceOrderHandler> _logger;

    /// <summary>Creates the handler.</summary>
    public PlaceOrderHandler(IEventStore eventStore, ILogger<PlaceOrderHandler> logger)
    {
        _eventStore = eventStore;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task HandleAsync(PlaceOrderCommand command, CancellationToken cancellationToken = default)
    {
        var streamId = $"Order:{command.Id}";
        var evt = new OrderPlaced(Guid.NewGuid(), DateTime.UtcNow, command.CustomerId, command.Items);

        await _eventStore.AppendAsync(streamId, [evt], cancellationToken: cancellationToken);

        _logger.LogInformation(
            "  [ordering] Order {OrderId} placed for customer {CustomerId} with {Count} item(s)",
            command.Id, command.CustomerId, command.Items.Count);
    }
}

/// <summary>
/// Handles <see cref="FulfillOrderCommand"/>.
/// Injects <see cref="IEventStore"/> the same way as <see cref="PlaceOrderHandler"/>.
/// </summary>
public sealed class FulfillOrderHandler : ICommandHandler<FulfillOrderCommand>
{
    private readonly IEventStore _eventStore;
    private readonly ILogger<FulfillOrderHandler> _logger;

    /// <summary>Creates the handler.</summary>
    public FulfillOrderHandler(IEventStore eventStore, ILogger<FulfillOrderHandler> logger)
    {
        _eventStore = eventStore;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task HandleAsync(FulfillOrderCommand command, CancellationToken cancellationToken = default)
    {
        var streamId = $"Order:{command.Id}";
        var evt = new OrderFulfilled(Guid.NewGuid(), DateTime.UtcNow);

        await _eventStore.AppendAsync(streamId, [evt], cancellationToken: cancellationToken);

        _logger.LogInformation("  [ordering] Order {OrderId} fulfilled", command.Id);
    }
}
