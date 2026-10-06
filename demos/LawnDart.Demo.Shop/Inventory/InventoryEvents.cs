using LawnDart;
using LawnDart.EventStore;

namespace LawnDart.Demo.Shop.Inventory;

[EventTypeName("stock-reserved")]
public record StockReserved( Guid Id, DateTime Timestamp, Guid ProductId, Guid OrderId, int Quantity) : IEvent;
