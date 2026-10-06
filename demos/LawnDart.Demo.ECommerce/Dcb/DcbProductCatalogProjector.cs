using LawnDart;
using LawnDart.Demo.ECommerce.Domain.Product.Events;
using LawnDart.Demo.ECommerce.Projections;
using LawnDart.EventStore;

namespace LawnDart.Demo.ECommerce.Dcb;

/// <summary>
/// Product catalog projector using DCB approach.
/// Queries events by tags instead of reading from streams.
/// </summary>
public class DcbProductCatalogProjector
{
    private readonly IEventStore _eventStore;

    public DcbProductCatalogProjector(IEventStore eventStore)
    {
        _eventStore = eventStore;
    }

    public async Task<ProductCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)
    {
        var query = Query.FromItems(QueryItem.ByType(EventTypeCatalog.TryGetDeclaredName(typeof(ProductCreated))!));
        var allProductEvents = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(query, cancellationToken: cancellationToken))
            allProductEvents.Add(evt);

        // Group events by product ID (from tags)
        var productGroups = allProductEvents
            .SelectMany(e => e.Tags
                .Where(t => t.StartsWith("product:"))
                .Select(t => new { Tag = t, Event = e }))
            .GroupBy(x => x.Tag)
            .ToList();

        var catalog = new ProductCatalog();

        foreach (var group in productGroups)
        {
            var productId = ExtractProductIdFromTag(group.Key);
            if (productId == null) continue;

            var events = group.Select(g => g.Event).OrderBy(e => e.SequencePosition);
            var productView = ReplayToProductView(productId.Value, events);

            if (productView != null)
            {
                catalog.Products[productId.Value.ToString()] = productView;
            }
        }

        return catalog;
    }

    private ProductView? ReplayToProductView(Guid productId, IEnumerable<SequencedEvent> events)
    {
        ProductView? view = null;

        foreach (var sequencedEvent in events)
        {
            switch (sequencedEvent.Event)
            {
                case ProductCreated created:
                    if (created.ProductId == productId)
                    {
                        view = new ProductView
                        {
                            ProductId = created.ProductId,
                            Name = created.Name,
                            Sku = created.Sku,
                            Price = created.Price,
                            Inventory = created.InitialInventory
                        };
                    }
                    break;

                case PriceUpdated priceUpdated:
                    if (view != null)
                    {
                        view.Price = priceUpdated.NewPrice;
                    }
                    break;

                case InventoryUpdated inventoryUpdated:
                    if (view != null)
                    {
                        view.Inventory = inventoryUpdated.NewQuantity;
                    }
                    break;
            }
        }

        return view;
    }

    private Guid? ExtractProductIdFromTag(string tag)
    {
        // Tag format: "product:{guid}"
        var parts = tag.Split(':');
        if (parts.Length > 1 && Guid.TryParse(parts[1], out var id))
        {
            return id;
        }
        return null;
    }
}

