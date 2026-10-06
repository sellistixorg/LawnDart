using LawnDart;
using LawnDart.EventStore;

namespace LawnDart.Demo.Shop.Domain.Product.Events;

[EventTypeName("product-created")]
public record ProductCreated( Guid Id, DateTime Timestamp, Guid ProductId, string Name, string Description, decimal Price, int InitialStock, string Sku, string SellerId, string SellerName) : IEvent;

[EventTypeName("sku-reserved")]
public record SkuReserved( Guid Id, DateTime Timestamp, string Sku, Guid ProductId, string TenantId) : IEvent;

[EventTypeName("stock-updated")]
public record StockUpdated( Guid Id, DateTime Timestamp, Guid ProductId, int NewStock, int Delta) : IEvent;

[EventTypeName("price-updated")]
public record PriceUpdated( Guid Id, DateTime Timestamp, Guid ProductId, decimal NewPrice) : IEvent;

/// <summary>
/// Emitted by <see cref="InventoryAggregate"/> when stock is first created for a product.
/// Separate from <see cref="ProductCreated"/>. Inventory is its own stream.
/// </summary>
[EventTypeName("inventory-initialized")]
public record InventoryInitialized( Guid Id, DateTime Timestamp, Guid ProductId, int InitialQuantity) : IEvent;
