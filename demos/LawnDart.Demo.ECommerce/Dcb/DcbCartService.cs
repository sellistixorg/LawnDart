using LawnDart;
using LawnDart.Demo.ECommerce.Domain.Cart;
using LawnDart.Demo.ECommerce.Domain.Cart.Events;
using LawnDart.EventStore;
using LawnDart.Metadata;
using ConcurrencyException = LawnDart.EventStore.ConcurrencyException;

namespace LawnDart.Demo.ECommerce.Dcb;

/// <summary>
/// Cart service using DCB approach.
/// Uses tag-based queries instead of stream-based access.
/// </summary>
public class DcbCartService
{
    private readonly IEventStore _eventStore;
    private readonly IMetadataProvider _metadataProvider;

    public DcbCartService(IEventStore eventStore, IMetadataProvider metadataProvider)
    {
        _eventStore = eventStore;
        _metadataProvider = metadataProvider;
    }

    public async Task<CartState> GetCartStateAsync(Guid cartId)
    {
        // Query by tag instead of reading stream
        var query = Query.FromItems(QueryItem.ByTags($"cart:{cartId}"));
        var events = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(query))
            events.Add(evt);

        // Replay events to build state
        return ReplayCartEvents(events.OrderBy(e => e.SequencePosition));
    }

    public async Task CreateCartAsync(Guid cartId, string customerId)
    {
        var lastPosition = await GetLastSequencePositionAsync($"cart:{cartId}");
        
        // Create append condition
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags($"cart:{cartId}")),
            after: lastPosition);

        var @event = new CartCreated(Guid.NewGuid(), DateTime.UtcNow, cartId, customerId);
        var metadata = CreateEventMetadata();
        
        // The store assigns the stream id. The entity tag is what queries use. Extra tag: cart:{cartId} for querying
        var operationId = Guid.NewGuid();
        var operationTag = $"operation:{operationId}";

        await _eventStore.AppendAsync(
            new[] { @event },
            condition,
            metadata,
            tags: new[] { operationTag, $"cart:{cartId}", $"customer:{customerId}" });
    }

    public async Task AddItemAsync(Guid cartId, string productId, int quantity)
    {
        var lastPosition = await GetLastSequencePositionAsync($"cart:{cartId}");
        
        // Create append condition
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags($"cart:{cartId}")),
            after: lastPosition);

        var @event = new ItemAddedToCart(Guid.NewGuid(), DateTime.UtcNow, productId, quantity);
        var metadata = CreateEventMetadata();
        
        // The store assigns the stream id. The entity tag is what queries use. Extra tag: cart:{cartId} for querying
        var operationId = Guid.NewGuid();
        var operationTag = $"operation:{operationId}";

        await _eventStore.AppendAsync(
            new[] { @event },
            condition,
            metadata,
            tags: new[] { operationTag, $"cart:{cartId}", $"product:{productId}" });
    }

    public async Task RemoveItemAsync(Guid cartId, string productId)
    {
        var lastPosition = await GetLastSequencePositionAsync($"cart:{cartId}");
        
        // Create append condition
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags($"cart:{cartId}")),
            after: lastPosition);

        var @event = new ItemRemovedFromCart(Guid.NewGuid(), DateTime.UtcNow, productId);
        var metadata = CreateEventMetadata();
        
        // The store assigns the stream id. The entity tag is what queries use. Extra tag: cart:{cartId} for querying
        var operationId = Guid.NewGuid();
        var operationTag = $"operation:{operationId}";

        await _eventStore.AppendAsync(
            new[] { @event },
            condition,
            metadata,
            tags: new[] { operationTag, $"cart:{cartId}", $"product:{productId}" });
    }

    public async Task CheckoutCartAsync(Guid cartId)
    {
        var lastPosition = await GetLastSequencePositionAsync($"cart:{cartId}");
        
        // Create append condition
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags($"cart:{cartId}")),
            after: lastPosition);

        var orderId = Guid.NewGuid();
        var @event = new CartCheckedOut(Guid.NewGuid(), DateTime.UtcNow, orderId);
        var metadata = CreateEventMetadata();
        
        // The store assigns the stream id. The entity tag is what queries use. Extra tag: cart:{cartId} for querying
        var operationId = Guid.NewGuid();
        var operationTag = $"operation:{operationId}";

        await _eventStore.AppendAsync(
            new[] { @event },
            condition,
            metadata,
            tags: new[] { operationTag, $"cart:{cartId}", $"order:{orderId}" });
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

    private CartState ReplayCartEvents(IEnumerable<SequencedEvent> events)
    {
        var state = new CartState();

        foreach (var sequencedEvent in events)
        {
            switch (sequencedEvent.Event)
            {
                case CartCreated created:
                    state.CartId = created.CartId;
                    state.CustomerId = created.CustomerId;
                    state.Items = new List<CartItem>();
                    state.IsCheckedOut = false;
                    break;

                case ItemAddedToCart itemAdded:
                    var existingItem = state.Items.FirstOrDefault(i => i.ProductId == itemAdded.ProductId);
                    if (existingItem != null)
                    {
                        existingItem.Quantity += itemAdded.Quantity;
                    }
                    else
                    {
                        state.Items.Add(new CartItem
                        {
                            ProductId = itemAdded.ProductId,
                            Quantity = itemAdded.Quantity
                        });
                    }
                    break;

                case ItemRemovedFromCart itemRemoved:
                    var itemToRemove = state.Items.FirstOrDefault(i => i.ProductId == itemRemoved.ProductId);
                    if (itemToRemove != null)
                    {
                        state.Items.Remove(itemToRemove);
                    }
                    break;

                case CartCheckedOut checkedOut:
                    state.IsCheckedOut = true;
                    state.OrderId = checkedOut.OrderId;
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

