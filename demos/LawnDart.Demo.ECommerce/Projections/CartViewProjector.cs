using LawnDart;
using LawnDart.EventStore;
using LawnDart.Patterns.Projection;
using LawnDart.Demo.ECommerce.Domain.Cart.Events;

namespace LawnDart.Demo.ECommerce.Projections;

#pragma warning disable LAWNDART001 // Legacy hand-rolled fold; author ProjectionBase instead.
public class CartViewProjector : IProjector<CartView>
{
    private readonly Dictionary<string, CartView> _views = new();
    private readonly IEventStore _eventStore;
    private readonly Dictionary<string, decimal> _productPrices = new(); // Simple price lookup

    public CartViewProjector(IEventStore eventStore)
    {
        _eventStore = eventStore;
    }

    public Task ProjectAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default) where TEvent : IEvent
    {
        // This method is part of the interface but we use ProjectSequencedEventAsync for manual execution
        return Task.CompletedTask;
    }

    public Task ProjectSequencedEventAsync(SequencedEvent sequencedEvent, CancellationToken cancellationToken = default)
    {
        var @event = sequencedEvent.Event;
        var streamId = sequencedEvent.StreamId;

        if (!_views.TryGetValue(streamId, out var view))
        {
            view = new CartView { CartId = ExtractCartId(streamId) };
            _views[streamId] = view;
        }

        switch (@event)
        {
            case CartCreated created:
                view.CartId = created.CartId;
                view.CustomerId = created.CustomerId;
                view.Items = new List<CartItemView>();
                view.Total = 0m;
                view.ItemCount = 0;
                view.IsCheckedOut = false;
                break;

            case ItemAddedToCart added:
                var existingItem = view.Items.FirstOrDefault(i => i.ProductId == added.ProductId);
                if (existingItem != null)
                {
                    existingItem.Quantity += added.Quantity;
                }
                else
                {
                    view.Items.Add(new CartItemView
                    {
                        ProductId = added.ProductId,
                        Quantity = added.Quantity
                    });
                }
                view.ItemCount += added.Quantity;
                view.Total += CalculateItemTotal(added.ProductId, added.Quantity);
                break;

            case ItemRemovedFromCart removed:
                var item = view.Items.FirstOrDefault(i => i.ProductId == removed.ProductId);
                if (item != null)
                {
                    view.ItemCount -= item.Quantity;
                    view.Total -= CalculateItemTotal(removed.ProductId, item.Quantity);
                    view.Items.Remove(item);
                }
                break;

            case CartCheckedOut checkedOut:
                view.IsCheckedOut = true;
                view.OrderId = checkedOut.OrderId;
                break;
        }

        return Task.CompletedTask;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var cartStreams = await _eventStore.GetStreamsByAggregateTypeAsync("Cart", cancellationToken);

        foreach (var stream in cartStreams)
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
            if (!stream.StreamId.Contains(":Cart:") && !stream.StreamId.StartsWith("Cart:"))
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

    public Task<CartView?> GetAsync(string cartId, CancellationToken cancellationToken = default)
    {
        // Find view by cartId (convert string to Guid for comparison)
        if (Guid.TryParse(cartId, out var cartGuid))
        {
            var view = _views.Values.FirstOrDefault(v => v.CartId == cartGuid);
            return Task.FromResult(view);
        }
        return Task.FromResult<CartView?>(null);
    }

    public Task<IEnumerable<CartView>> GetAllAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_views.Values.AsEnumerable());
    }

    public void SetProductPrice(string productId, decimal price)
    {
        _productPrices[productId] = price;
    }

    private Guid ExtractCartId(string streamId)
    {
        // Stream format: {tenantId}:{aggregateType}:{aggregateId}
        // We want the aggregateId (last part)
        var parts = streamId.Split(':');
        var idString = parts.Length >= 3 ? parts[2] :      // Tenant-prefixed format
                       parts.Length == 2 ? parts[1] :       // Non-tenant format
                       string.Empty;
        return Guid.TryParse(idString, out var id) ? id : Guid.Empty;
    }

    private decimal CalculateItemTotal(string productId, int quantity)
    {
        if (_productPrices.TryGetValue(productId, out var price))
        {
            return quantity * price;
        }
        return quantity * 10.00m; // Default placeholder price
    }
}


