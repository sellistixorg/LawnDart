using LawnDart;
using LawnDart.Demo.ECommerce.Domain.Order;
using LawnDart.Demo.ECommerce.Domain.Order.Events;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Demo.ECommerce.Dcb;

/// <summary>
/// Order service using DCB approach.
/// Uses tag-based queries instead of stream-based access.
/// </summary>
public class DcbOrderService
{
    private readonly IEventStore _eventStore;
    private readonly IMetadataProvider _metadataProvider;

    public DcbOrderService(IEventStore eventStore, IMetadataProvider metadataProvider)
    {
        _eventStore = eventStore;
        _metadataProvider = metadataProvider;
    }

    public async Task<OrderState> GetOrderStateAsync(Guid orderId)
    {
        // Query by tag instead of reading stream
        var query = Query.FromItems(QueryItem.ByTags($"order:{orderId}"));
        var events = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(query))
            events.Add(evt);

        // Replay events to build state
        return ReplayOrderEvents(events.OrderBy(e => e.SequencePosition));
    }

    public async Task CreateOrderAsync(Guid orderId, Guid cartId, string customerId)
    {
        var lastPosition = await GetLastSequencePositionAsync($"order:{orderId}");
        
        // Create append condition
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags($"order:{orderId}")),
            after: lastPosition);

        var @event = new OrderCreated(Guid.NewGuid(), DateTime.UtcNow, orderId, cartId, customerId);
        var metadata = CreateEventMetadata();
        
        // The store assigns the stream id. The entity tag is what queries use. Extra tag: order:{orderId} for querying
        var operationId = Guid.NewGuid();
        var operationTag = $"operation:{operationId}";

        await _eventStore.AppendAsync(
            new[] { @event },
            condition,
            metadata,
            tags: new[] { operationTag, $"order:{orderId}", $"cart:{cartId}", $"customer:{customerId}" });
    }

    public async Task AddOrderItemAsync(Guid orderId, string productId, int quantity, decimal unitPrice)
    {
        var lastPosition = await GetLastSequencePositionAsync($"order:{orderId}");
        
        // Create append condition
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags($"order:{orderId}")),
            after: lastPosition);

        var @event = new OrderItemAdded(Guid.NewGuid(), DateTime.UtcNow, productId, quantity, unitPrice);
        var metadata = CreateEventMetadata();
        
        // The store assigns the stream id. The entity tag is what queries use. Extra tag: order:{orderId} for querying
        var operationId = Guid.NewGuid();
        var operationTag = $"operation:{operationId}";

        await _eventStore.AppendAsync(
            new[] { @event },
            condition,
            metadata,
            tags: new[] { operationTag, $"order:{orderId}", $"product:{productId}" });
    }

    public async Task CompleteOrderAsync(Guid orderId)
    {
        var lastPosition = await GetLastSequencePositionAsync($"order:{orderId}");
        
        // Create append condition
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags($"order:{orderId}")),
            after: lastPosition);

        var @event = new OrderCompleted(Guid.NewGuid(), DateTime.UtcNow);
        var metadata = CreateEventMetadata();
        
        // The store assigns the stream id. The entity tag is what queries use. Extra tag: order:{orderId} for querying
        var operationId = Guid.NewGuid();
        var operationTag = $"operation:{operationId}";

        await _eventStore.AppendAsync(
            new[] { @event },
            condition,
            metadata,
            tags: new[] { operationTag, $"order:{orderId}" });
    }

    private async Task<long> GetLastSequencePositionAsync(string tag)
    {
        var query = Query.FromItems(QueryItem.ByTags(tag));
        var maxSeq = 0L;
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(query))
        {
            if (evt.SequencePosition > maxSeq)
                maxSeq = evt.SequencePosition;
        }
        return maxSeq;
    }

    private OrderState ReplayOrderEvents(IEnumerable<SequencedEvent> events)
    {
        var state = new OrderState();

        foreach (var sequencedEvent in events)
        {
            switch (sequencedEvent.Event)
            {
                case OrderCreated created:
                    state.OrderId = created.OrderId;
                    state.CartId = created.CartId;
                    state.CustomerId = created.CustomerId;
                    state.Items = new List<OrderItem>();
                    state.IsCompleted = false;
                    state.Total = 0m;
                    break;

                case OrderItemAdded itemAdded:
                    state.Items.Add(new OrderItem
                    {
                        ProductId = itemAdded.ProductId,
                        Quantity = itemAdded.Quantity,
                        UnitPrice = itemAdded.UnitPrice
                    });
                    state.Total += itemAdded.Quantity * itemAdded.UnitPrice;
                    break;

                case OrderCompleted:
                    state.IsCompleted = true;
                    break;
            }
        }

        return state;
    }

    private EventMetadata CreateEventMetadata()
    {
        var commandMetadata = _metadataProvider.CaptureCommandMetadata();
        return new EventMetadata
        {
            UserId = commandMetadata.UserId,
            TenantId = commandMetadata.TenantId,
            CorrelationId = commandMetadata.CorrelationId,
            EventId = Guid.NewGuid().ToString(),
            Timestamp = DateTime.UtcNow
        };
    }
}

