using LawnDart;
using LawnDart.Demo.ECommerce.Domain.Cart.Events;
using LawnDart.Demo.ECommerce.Projections;
using LawnDart.EventStore;

namespace LawnDart.Demo.ECommerce.Dcb;

/// <summary>
/// Cart view projector using DCB approach.
/// Queries events by tags instead of reading from streams.
/// </summary>
public class DcbCartViewProjector
{
    private readonly IEventStore _eventStore;
    private readonly Dictionary<string, decimal> _productPrices = new();

    public DcbCartViewProjector(IEventStore eventStore)
    {
        _eventStore = eventStore;
    }

    public async Task<CartView?> GetAsync(Guid cartId, CancellationToken cancellationToken = default)
    {
        // Query by tag instead of reading stream
        var query = Query.FromItems(QueryItem.ByTags($"cart:{cartId}"));
        var events = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(query, cancellationToken: cancellationToken))
            events.Add(evt);

        // Replay events to build view
        return ReplayToCartView(events.OrderBy(e => e.SequencePosition));
    }

    public void SetProductPrice(string productId, decimal price)
    {
        _productPrices[productId] = price;
    }

    private CartView? ReplayToCartView(IEnumerable<SequencedEvent> events)
    {
        CartView? view = null;

        foreach (var sequencedEvent in events)
        {
            if (view == null)
            {
                view = new CartView { Items = new List<CartItemView>() };
            }

            switch (sequencedEvent.Event)
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
        }

        return view;
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

