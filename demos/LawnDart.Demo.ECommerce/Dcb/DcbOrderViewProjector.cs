using LawnDart;
using LawnDart.Demo.ECommerce.Domain.Order.Events;
using LawnDart.Demo.ECommerce.Projections;
using LawnDart.EventStore;

namespace LawnDart.Demo.ECommerce.Dcb;

/// <summary>
/// Order view projector using DCB approach.
/// Queries events by tags instead of reading from streams.
/// </summary>
public class DcbOrderViewProjector
{
    private readonly IEventStore _eventStore;

    public DcbOrderViewProjector(IEventStore eventStore)
    {
        _eventStore = eventStore;
    }

    public async Task<OrderView?> GetAsync(Guid orderId, CancellationToken cancellationToken = default)
    {
        // Query by tag instead of reading stream
        var query = Query.FromItems(QueryItem.ByTags($"order:{orderId}"));
        var events = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(query, cancellationToken: cancellationToken))
            events.Add(evt);

        // Replay events to build view
        return ReplayToOrderView(events.OrderBy(e => e.SequencePosition));
    }

    private OrderView? ReplayToOrderView(IEnumerable<SequencedEvent> events)
    {
        OrderView? view = null;

        foreach (var sequencedEvent in events)
        {
            if (view == null)
            {
                view = new OrderView { Items = new List<OrderItemView>() };
            }

            switch (sequencedEvent.Event)
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
        }

        return view;
    }
}

