using LawnDart;
using LawnDart.EventStore;
using LawnDart.Patterns.Projection;
using LawnDart.Demo.ECommerce.Domain.Order.Events;

namespace LawnDart.Demo.ECommerce.Projections;

#pragma warning disable LAWNDART001 // Legacy hand-rolled fold; author ProjectionBase instead.
public class OrderViewProjector : IProjector<OrderView>
{
    private readonly Dictionary<string, OrderView> _views = new();
    private readonly IEventStore _eventStore;

    public OrderViewProjector(IEventStore eventStore)
    {
        _eventStore = eventStore;
    }

    public Task ProjectAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default) where TEvent : IEvent
    {
        return Task.CompletedTask;
    }

    public Task ProjectSequencedEventAsync(SequencedEvent sequencedEvent, CancellationToken cancellationToken = default)
    {
        var @event = sequencedEvent.Event;
        var streamId = sequencedEvent.StreamId;

        if (!_views.TryGetValue(streamId, out var view))
        {
            view = new OrderView { OrderId = ExtractOrderId(streamId) };
            _views[streamId] = view;
        }

        switch (@event)
        {
            case OrderCreated created:
                view.OrderId = created.OrderId;
                view.CartId = created.CartId;
                view.CustomerId = created.CustomerId;
                view.Items = new List<OrderItemView>();
                view.Total = 0m;
                view.IsCompleted = false;
                break;

            case OrderItemAdded itemAdded:
                view.Items.Add(new OrderItemView
                {
                    ProductId = itemAdded.ProductId,
                    Quantity = itemAdded.Quantity,
                    UnitPrice = itemAdded.UnitPrice,
                    LineTotal = itemAdded.Quantity * itemAdded.UnitPrice
                });
                view.Total += itemAdded.Quantity * itemAdded.UnitPrice;
                break;

            case OrderCompleted:
                view.IsCompleted = true;
                break;
        }

        return Task.CompletedTask;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var orderStreams = await _eventStore.GetStreamsByAggregateTypeAsync("Order", cancellationToken);

        foreach (var stream in orderStreams)
        {
            var events = await _eventStore.ReadStreamAsync(stream.StreamId, cancellationToken: cancellationToken);

            foreach (var sequencedEvent in events)
            {
                await ProjectSequencedEventAsync(sequencedEvent, cancellationToken);
            }
        }
    }

    public async Task ProcessNewEventsAsync(
        long lastProcessedPosition,
        CancellationToken cancellationToken = default)
    {
        var updatedStreams = await _eventStore.GetStreamsUpdatedAfterAsync(
            lastProcessedPosition,
            cancellationToken: cancellationToken);

        foreach (var stream in updatedStreams)
        {
            // Handle both tenant-prefixed and non-tenant formats
            if (!stream.StreamId.Contains(":Order:") && !stream.StreamId.StartsWith("Order:"))
                continue;

            var events = await _eventStore.ReadStreamAsync(
                stream.StreamId,
                cancellationToken: cancellationToken);

            foreach (var sequencedEvent in events)
            {
                if (sequencedEvent.SequencePosition > lastProcessedPosition)
                {
                    await ProjectSequencedEventAsync(sequencedEvent, cancellationToken);
                }
            }
        }
    }

    public Task<OrderView?> GetAsync(string orderId, CancellationToken cancellationToken = default)
    {
        // Find view by orderId (convert string to Guid for comparison)
        if (Guid.TryParse(orderId, out var orderGuid))
        {
            var view = _views.Values.FirstOrDefault(v => v.OrderId == orderGuid);
            return Task.FromResult(view);
        }
        return Task.FromResult<OrderView?>(null);
    }

    public Task<IEnumerable<OrderView>> GetAllAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_views.Values.AsEnumerable());
    }

    private Guid ExtractOrderId(string streamId)
    {
        // Stream format: {tenantId}:{aggregateType}:{aggregateId}
        // We want the aggregateId (last part)
        var parts = streamId.Split(':');
        var idString = parts.Length >= 3 ? parts[2] :      // Tenant-prefixed format
                       parts.Length == 2 ? parts[1] :       // Non-tenant format
                       string.Empty;
        return Guid.TryParse(idString, out var id) ? id : Guid.Empty;
    }
}


