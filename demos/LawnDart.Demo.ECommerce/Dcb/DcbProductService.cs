using LawnDart;
using LawnDart.Demo.ECommerce.Domain.Product;
using LawnDart.Demo.ECommerce.Domain.Product.Events;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Demo.ECommerce.Dcb;

/// <summary>
/// Product service using DCB approach.
/// Uses tag-based queries instead of stream-based access.
/// </summary>
public class DcbProductService
{
    private readonly IEventStore _eventStore;
    private readonly IMetadataProvider _metadataProvider;

    public DcbProductService(IEventStore eventStore, IMetadataProvider metadataProvider)
    {
        _eventStore = eventStore;
        _metadataProvider = metadataProvider;
    }

    public async Task<ProductState> GetProductStateAsync(Guid productId)
    {
        // Query by tag instead of reading stream
        var query = Query.FromItems(QueryItem.ByTags($"product:{productId}"));
        var events = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(query))
            events.Add(evt);

        // Replay events to build state
        return ReplayProductEvents(events.OrderBy(e => e.SequencePosition));
    }

    public async Task CreateProductAsync(Guid productId, string name, string sku, decimal price, int initialInventory)
    {
        var lastPosition = await GetLastSequencePositionAsync($"product:{productId}");
        
        // Create append condition
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags($"product:{productId}")),
            after: lastPosition);

        var @event = new ProductCreated(Guid.NewGuid(), DateTime.UtcNow, productId, name, sku, price, initialInventory);
        var metadata = CreateEventMetadata();
        
        // The store assigns the stream id. The entity tag is what queries use. Extra tag: product:{productId} for querying
        var operationId = Guid.NewGuid();
        var operationTag = $"operation:{operationId}";

        await _eventStore.AppendAsync(
            new[] { @event },
            condition,
            metadata,
            tags: new[] { operationTag, $"product:{productId}" });
    }

    public async Task UpdatePriceAsync(Guid productId, decimal newPrice)
    {
        var lastPosition = await GetLastSequencePositionAsync($"product:{productId}");
        
        // Create append condition
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags($"product:{productId}")),
            after: lastPosition);

        var @event = new PriceUpdated(Guid.NewGuid(), DateTime.UtcNow, newPrice);
        var metadata = CreateEventMetadata();
        
        // The store assigns the stream id. The entity tag is what queries use. Extra tag: product:{productId} for querying
        var operationId = Guid.NewGuid();
        var operationTag = $"operation:{operationId}";

        await _eventStore.AppendAsync(
            new[] { @event },
            condition,
            metadata,
            tags: new[] { operationTag, $"product:{productId}" });
    }

    public async Task UpdateInventoryAsync(Guid productId, int quantityDelta)
    {
        var lastPosition = await GetLastSequencePositionAsync($"product:{productId}");
        
        // Create append condition
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags($"product:{productId}")),
            after: lastPosition);

        // Get current state to validate
        var currentState = await GetProductStateAsync(productId);
        var newQuantity = currentState.Inventory + quantityDelta;
        
        if (newQuantity < 0)
        {
            throw new InvalidOperationException($"Insufficient inventory. Current: {currentState.Inventory}, Requested change: {quantityDelta}");
        }

        var @event = new InventoryUpdated(Guid.NewGuid(), DateTime.UtcNow, newQuantity, quantityDelta);
        var metadata = CreateEventMetadata();
        
        // The store assigns the stream id. The entity tag is what queries use. Extra tag: product:{productId} for querying
        var operationId = Guid.NewGuid();
        var operationTag = $"operation:{operationId}";

        await _eventStore.AppendAsync(
            new[] { @event },
            condition,
            metadata,
            tags: new[] { operationTag, $"product:{productId}" });
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

    private ProductState ReplayProductEvents(IEnumerable<SequencedEvent> events)
    {
        var state = new ProductState();

        foreach (var sequencedEvent in events)
        {
            switch (sequencedEvent.Event)
            {
                case ProductCreated created:
                    state.ProductId = created.ProductId;
                    state.Name = created.Name;
                    state.Sku = created.Sku;
                    state.Price = created.Price;
                    state.Inventory = created.InitialInventory;
                    break;

                case PriceUpdated priceUpdated:
                    state.Price = priceUpdated.NewPrice;
                    break;

                case InventoryUpdated inventoryUpdated:
                    state.Inventory = inventoryUpdated.NewQuantity;
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

