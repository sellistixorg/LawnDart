using LawnDart.EventStore;
using LawnDart.Demo.ECommerce.Projections;

namespace LawnDart.Demo.ECommerce;

public class ProjectionRunner
{
    private readonly IEventStore _eventStore;
    private readonly CartViewProjector _cartProjector;
    private readonly OrderViewProjector _orderProjector;
    private readonly ProductCatalogProjector _productProjector;

    public ProjectionRunner(
        IEventStore eventStore,
        CartViewProjector cartProjector,
        OrderViewProjector orderProjector,
        ProductCatalogProjector productProjector)
    {
        _eventStore = eventStore;
        _cartProjector = cartProjector;
        _orderProjector = orderProjector;
        _productProjector = productProjector;
    }

    public async Task ProjectEventsAsync(string streamId, CancellationToken cancellationToken = default)
    {
        var events = await _eventStore.ReadStreamAsync(streamId, cancellationToken: cancellationToken);

        // Support both plain "Cart:{id}" and tenant-prefixed "{tenant}:Cart:{id}" stream IDs
        if (streamId.StartsWith("Cart:") || streamId.Contains(":Cart:"))
        {
            foreach (var evt in events)
                await _cartProjector.ProjectSequencedEventAsync(evt, cancellationToken);
        }
        else if (streamId.StartsWith("Order:") || streamId.Contains(":Order:"))
        {
            foreach (var evt in events)
                await _orderProjector.ProjectSequencedEventAsync(evt, cancellationToken);
        }
        else if (streamId.StartsWith("Product:") || streamId.Contains(":Product:"))
        {
            foreach (var evt in events)
                await _productProjector.ProjectProductEventAsync(evt, cancellationToken);
        }
    }

    public async Task InitializeAllProjectionsAsync(CancellationToken cancellationToken = default)
    {
        await _productProjector.InitializeAsync(cancellationToken);
        await _cartProjector.InitializeAsync(cancellationToken);
        await _orderProjector.InitializeAsync(cancellationToken);
    }
}


