using LawnDart;
using LawnDart.EventStore;
using LawnDart.Patterns.Projection;
using LawnDart.Demo.ECommerce.Domain.Product.Events;

namespace LawnDart.Demo.ECommerce.Projections;

#pragma warning disable LAWNDART001 // Legacy hand-rolled fold; author ProjectionBase instead.
public class ProductCatalogProjector : IProjector<ProductCatalog>
{
    private readonly ProductCatalog _catalog = new();
    private readonly IEventStore _eventStore;

    public ProductCatalogProjector(IEventStore eventStore)
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

        switch (@event)
        {
            case ProductCreated created:
                _catalog.Products[created.ProductId.ToString()] = new ProductView
                {
                    ProductId = created.ProductId,
                    Name = created.Name,
                    Sku = created.Sku,
                    Price = created.Price,
                    Inventory = created.InitialInventory
                };
                break;

            case PriceUpdated priceUpdated:
                // Find product by ID - need to search through dictionary
                var productToUpdate = _catalog.Products.Values.FirstOrDefault(p => 
                    priceUpdated.Id == Guid.Empty || // This won't work, need stream context
                    true); // Placeholder - in real implementation would use streamId to find product
                // For now, we'll need to track product ID differently or use stream ID
                break;

            case InventoryUpdated inventoryUpdated:
                // Similar issue - need stream context to find product
                break;
        }

        return Task.CompletedTask;
    }

    public Task ProjectProductEventAsync(SequencedEvent sequencedEvent, CancellationToken cancellationToken = default)
    {
        var @event = sequencedEvent.Event;
        var streamId = sequencedEvent.StreamId;
        var productId = ExtractProductId(streamId);

        switch (@event)
        {
            case ProductCreated created:
                _catalog.Products[productId] = new ProductView
                {
                    ProductId = created.ProductId,
                    Name = created.Name,
                    Sku = created.Sku,
                    Price = created.Price,
                    Inventory = created.InitialInventory
                };
                break;

            case PriceUpdated priceUpdated:
                if (_catalog.Products.TryGetValue(productId, out var product))
                {
                    product.Price = priceUpdated.NewPrice;
                }
                break;

            case InventoryUpdated inventoryUpdated:
                if (_catalog.Products.TryGetValue(productId, out var productForInventory))
                {
                    productForInventory.Inventory = inventoryUpdated.NewQuantity;
                }
                break;
        }

        return Task.CompletedTask;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var productStreams = await _eventStore.GetStreamsByAggregateTypeAsync("Product", cancellationToken);

        foreach (var stream in productStreams)
        {
            var events = await _eventStore.ReadStreamAsync(stream.StreamId, cancellationToken: cancellationToken);

            foreach (var sequencedEvent in events)
            {
                await ProjectProductEventAsync(sequencedEvent, cancellationToken);
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
            if (!stream.StreamId.Contains(":Product:") && !stream.StreamId.StartsWith("Product:"))
                continue;

            var events = await _eventStore.ReadStreamAsync(
                stream.StreamId,
                cancellationToken: cancellationToken);

            foreach (var sequencedEvent in events)
            {
                if (sequencedEvent.SequencePosition > lastProcessedPosition)
                {
                    await ProjectProductEventAsync(sequencedEvent, cancellationToken);
                }
            }
        }
    }

    public Task<ProductCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_catalog);
    }

    public Task<ProductView?> GetProductAsync(string productId, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_catalog.Products.TryGetValue(productId, out var product) ? product : null);
    }

    private string ExtractProductId(string streamId)
    {
        // Stream format: {tenantId}:{aggregateType}:{aggregateId}
        // We want the aggregateId (last part)
        var parts = streamId.Split(':');
        return parts.Length >= 3 ? parts[2] : 
               parts.Length == 2 ? parts[1] : // Fallback for non-tenant format
               string.Empty;
    }
}


