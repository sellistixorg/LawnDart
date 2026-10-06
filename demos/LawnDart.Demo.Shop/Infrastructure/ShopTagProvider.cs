using LawnDart.Demo.Shop.Domain.Product.Events;
using LawnDart.Demo.Shop.Inventory;
using LawnDart.Tagging;
using IEvent = LawnDart.IEvent;

namespace LawnDart.Demo.Shop.Infrastructure;

/// <summary>
/// Provides event-store tags for shop domain events so that the DCB InventoryEntity
/// can replay its history via <c>QueryItem.ByTags(["product:{id}"])</c>.
///
/// Without these tags the DCB entity always starts from scratch (Stock = 0) because
/// <c>DcbRepository.GetOrCreateEntityAsync</c> queries events exclusively by tag.
/// </summary>
internal sealed class ShopTagProvider : ITagProvider
{
    public IEnumerable<string> GetTags(IEvent @event, object? context = null)
    {
        return @event switch
        {
            ProductCreated       e => [$"product:{e.ProductId}"],
            InventoryInitialized e => [$"product:{e.ProductId}"],
            StockUpdated         e => [$"product:{e.ProductId}"],
            PriceUpdated         e => [$"product:{e.ProductId}"],
            StockReserved        e => [$"product:{e.ProductId}"],
            _                      => []
        };
    }
}
