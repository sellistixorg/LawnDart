using LawnDart.Demo.Shop.Domain.Order.Events;
using LawnDart.Demo.Shop.Domain.Product.Events;
using LawnDart.Demo.Shop.Inventory;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Sdk;

namespace LawnDart.Demo.Shop.Projections;

// ── Views ────────────────────────────────────────────────────────────────────

/// <summary>
/// Per-product detail view : name, description, price, SKU, and seller identity.
/// Stock is intentionally absent: the <c>ProductCatalog</c> projection is a
/// <c>SingleStreamProjection</c> scoped to <c>ProductAggregate</c> streams only and
/// never receives inventory events (<c>InventoryInitialized</c>, <c>StockReserved</c>,
/// <c>StockUpdated</c>). For accurate real-time stock use the <c>AllProducts</c> global
/// projection (GET /api/views/products) which subscribes to all three stream types.
/// </summary>
public class ProductDetailView
{
    public Guid ProductId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public string Sku { get; set; } = string.Empty;
    public string SellerId { get; set; } = string.Empty;
    public string SellerName { get; set; } = string.Empty;
    public DateTime LastUpdated { get; set; }
}

/// <summary>
/// Catalogue summary view : same fields as <see cref="ProductDetailView"/> plus
/// <see cref="Stock"/> maintained by the <c>AllProducts</c> global projection.
/// Used by the shop UI and seller admin pages which read from GET /api/views/products.
/// </summary>
public class ProductCatalogView
{
    public Guid ProductId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Stock { get; set; }
    public string Sku { get; set; } = string.Empty;
    public string SellerId { get; set; } = string.Empty;
    public string SellerName { get; set; } = string.Empty;
    public DateTime LastUpdated { get; set; }
}

/// <summary>
/// Version 1 of the shop catalog read model. This intentionally excludes stock so the
/// versioned projection debugger can show how the richer v2 view evolved over time.
/// </summary>
public class ProductCatalogV1View
{
    public Guid ProductId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public string Sku { get; set; } = string.Empty;
    public string SellerId { get; set; } = string.Empty;
    public string SellerName { get; set; } = string.Empty;
    public DateTime LastUpdated { get; set; }
}

public class OrderSummaryView
{
    public Guid OrderId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string CustomerName { get; set; } = string.Empty;
    public Guid ProductId { get; set; }
    public string ProductName { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal Total => Quantity * UnitPrice;
    public string SellerId { get; set; } = string.Empty;
    public string SellerName { get; set; } = string.Empty;
    public Guid SellerFulfillmentId { get; set; }
    public string Status { get; set; } = "Pending";
    public string TenantId { get; set; } = string.Empty;
    public DateTime PlacedAt { get; set; }
    public DateTime LastUpdated { get; set; }
}

public class CustomerOrdersView
{
    public string CustomerId { get; set; } = string.Empty;
    public string CustomerName { get; set; } = string.Empty;
    public List<OrderSummaryView> Orders { get; set; } = new();
    public int TotalOrders { get; set; }
    public int PendingOrders { get; set; }
    public int CompletedOrders { get; set; }
    public DateTime LastUpdated { get; set; }
}

/// <summary>
/// Multi-stream view that combines order lifecycle events from <c>OrderAggregate</c> streams
/// with the <c>StockReserved</c> event from the DCB inventory stream.
/// This is the backing model for <see cref="OrderFulfillmentProjection"/>.
/// </summary>
public class OrderFulfillmentView
{
    public Guid OrderId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string CustomerName { get; set; } = string.Empty;
    public Guid ProductId { get; set; }
    public string ProductName { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal Total => Quantity * UnitPrice;
    public string SellerId { get; set; } = string.Empty;
    public string SellerName { get; set; } = string.Empty;
    public Guid SellerFulfillmentId { get; set; }

    /// <summary>UTC timestamp when the DCB <c>StockReserved</c> event was written (may be null until confirmed).</summary>
    public DateTime? StockReservedAt { get; set; }

    /// <summary>Payment reference from the <c>OrderPaymentProcessed</c> event.</summary>
    public string? PaymentReference { get; set; }
    public DateTime? PaidAt { get; set; }

    /// <summary>Carrier tracking number from the <c>OrderShipped</c> event.</summary>
    public string? TrackingNumber { get; set; }
    public DateTime? ShippedAt { get; set; }

    public string? CancellationReason { get; set; }

    /// <summary>Derived status summarising the fulfilment pipeline stage.</summary>
    public string FulfillmentStatus
    {
        get
        {
            if (CancellationReason is not null) return "Cancelled";
            if (ShippedAt.HasValue) return "Shipped";
            if (PaidAt.HasValue) return "AwaitingShipment";
            if (StockReservedAt.HasValue) return "AwaitingPayment";
            return "Pending";
        }
    }

    public DateTime PlacedAt { get; set; }
    public DateTime LastUpdated { get; set; }
}

/// <summary>SKU index view : maps per-seller SKU strings to product IDs.
/// Key format: "{SellerId}:{SKU}" (upper-case) to support multiple sellers sharing the same SKU.</summary>
public class ProductSkuIndexView
{
    /// <summary>Key: "{SellerId}:{SKU}" (upper-case). Value: ProductId.</summary>
    public Dictionary<string, Guid> SkuToProductId { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

// ── Projections ───────────────────────────────────────────────────────────────

/// <summary>
/// Per-product projection : product catalogue info only (name, description, price, sku, seller).
/// Stock levels are intentionally absent; use the <see cref="AllProductsProjection"/>
/// (GET /api/views/products) which subscribes to <c>InventoryInitialized</c>,
/// <c>StockUpdated</c>, and <c>StockReserved</c> events across all three stream types
/// for accurate real-time inventory.
/// Endpoint: GET /api/views/products/{productId}
/// </summary>
/// <remarks>
/// Uses <see cref="TenantScope.SystemGlobal"/> so that products created by any seller tenant
/// are visible to all customer tenants without a tenant-specific lookup.
/// </remarks>
[SingleStreamProjection("ProductCatalog", streamType: "ProductAggregate",
    tenantScope: TenantScope.SystemGlobal)]
[ProjectionEndpoint(
    route: "/api/views/products/{productId}",
    requiredPermission: null,
    cacheMaxAgeSeconds: 5)]
public class ProductCatalogProjection : ProjectionBase<ProductDetailView>
{
    public void Handle(ProductCreated e)
    {
        State.ProductId   = e.ProductId;
        State.Name        = e.Name;
        State.Description = e.Description;
        State.Price       = e.Price;
        State.Sku         = e.Sku;
        State.SellerId    = e.SellerId;
        State.SellerName  = e.SellerName;
        State.LastUpdated = e.Timestamp;
    }

    public void Handle(PriceUpdated e)
    {
        State.Price       = e.NewPrice;
        State.LastUpdated = e.Timestamp;
    }
}

/// <summary>
/// Per-order projection. One view per order stream (OrderAggregate:{orderId}).
/// Endpoint: GET /api/views/orders/{orderId}
/// </summary>
[SingleStreamProjection("OrderSummary", streamType: "OrderAggregate")]
[ProjectionEndpoint(
    route: "/api/views/orders/{orderId}",
    requiredPermission: null,
    cacheMaxAgeSeconds: 2)]
public class OrderSummaryProjection : ProjectionBase<OrderSummaryView>
{
    public void Handle(OrderPlaced e)
    {
        State.OrderId             = e.OrderId;
        State.CustomerId          = e.CustomerId;
        State.CustomerName        = e.CustomerName;
        State.ProductId           = e.ProductId;
        State.ProductName         = e.ProductName;
        State.Quantity            = e.Quantity;
        State.UnitPrice           = e.UnitPrice;
        State.SellerId            = e.SellerId;
        State.SellerName          = e.SellerName;
        State.SellerFulfillmentId = e.SellerFulfillmentId;
        State.Status              = "Pending";
        State.TenantId            = e.TenantId;
        State.PlacedAt            = e.Timestamp;
        State.LastUpdated         = e.Timestamp;
    }

    public void Handle(OrderPaymentProcessed e)
    {
        State.Status      = "PaymentProcessed";
        State.LastUpdated = e.Timestamp;
    }

    public void Handle(OrderShipped e)
    {
        State.Status      = "Shipped";
        State.LastUpdated = e.Timestamp;
    }

    public void Handle(OrderCancelled e)
    {
        State.Status      = "Cancelled";
        State.LastUpdated = e.Timestamp;
    }
}

/// <summary>
/// Global projection that builds a summary across ALL orders.
/// Used by the dashboard stats and to find overdue orders.
/// Sellers filter this by SellerId; buyers filter by CustomerId.
/// Endpoint: GET /api/views/orders
/// </summary>
[GlobalProjection("AllOrders")]
[ProjectionEndpoint(
    route: "/api/views/orders",
    requiredPermission: null,
    cacheMaxAgeSeconds: 2)]
public class AllOrdersProjection : ProjectionBase<List<OrderSummaryView>>
{
    public AllOrdersProjection()
    {
        State = new List<OrderSummaryView>();
    }

    public void Handle(OrderPlaced e)
    {
        State.Add(new OrderSummaryView
        {
            OrderId             = e.OrderId,
            CustomerId          = e.CustomerId,
            CustomerName        = e.CustomerName,
            ProductId           = e.ProductId,
            ProductName         = e.ProductName,
            Quantity            = e.Quantity,
            UnitPrice           = e.UnitPrice,
            SellerId            = e.SellerId,
            SellerName          = e.SellerName,
            SellerFulfillmentId = e.SellerFulfillmentId,
            Status              = "Pending",
            TenantId            = e.TenantId,
            PlacedAt            = e.Timestamp,
            LastUpdated         = e.Timestamp
        });
    }

    public void Handle(OrderPaymentProcessed e)
    {
        var order = State.FirstOrDefault(o => o.OrderId == e.OrderId);
        if (order is not null)
        {
            order.Status      = "PaymentProcessed";
            order.LastUpdated = e.Timestamp;
        }
    }

    public void Handle(OrderShipped e)
    {
        var order = State.FirstOrDefault(o => o.OrderId == e.OrderId);
        if (order is not null)
        {
            order.Status      = "Shipped";
            order.LastUpdated = e.Timestamp;
        }
    }

    public void Handle(OrderCancelled e)
    {
        var order = State.FirstOrDefault(o => o.OrderId == e.OrderId);
        if (order is not null)
        {
            order.Status      = "Cancelled";
            order.LastUpdated = e.Timestamp;
        }
    }
}

/// <summary>
/// Version 1 of the product catalog projection.
/// It tracks only product metadata and price changes, so callers cannot see live inventory.
/// </summary>
[GlobalProjection("AllProducts", Version = 1, DeprecationDateIso = "2026-12-31")]
[ProjectionEndpoint(
    route: "/api/views/products",
    requiredPermission: null,
    cacheMaxAgeSeconds: 5)]
public class AllProductsV1Projection : ProjectionBase<List<ProductCatalogV1View>>
{
    public AllProductsV1Projection()
    {
        State = new List<ProductCatalogV1View>();
    }

    public void Handle(ProductCreated e)
    {
        State.Add(new ProductCatalogV1View
        {
            ProductId = e.ProductId,
            Name = e.Name,
            Description = e.Description,
            Price = e.Price,
            Sku = e.Sku,
            SellerId = e.SellerId,
            SellerName = e.SellerName,
            LastUpdated = e.Timestamp
        });
    }

    public void Handle(PriceUpdated e)
    {
        var p = State.FirstOrDefault(p => p.ProductId == e.ProductId);
        if (p is not null)
        {
            p.Price = e.NewPrice;
            p.LastUpdated = e.Timestamp;
        }
    }
}

/// <summary>
/// Version 2 of the product catalog projection.
/// Reads events from both ProductAggregate (name/price/sku/seller) and InventoryAggregate (stock)
/// streams, plus DCB StockReserved events, to maintain accurate real-time stock levels.
/// Endpoint: GET /api/views/products
/// </summary>
[GlobalProjection("AllProducts", Version = 2, IsLatest = true)]
[ProjectionEndpoint(
    route: "/api/views/products",
    requiredPermission: null,
    cacheMaxAgeSeconds: 5)]
public class AllProductsProjection : ProjectionBase<List<ProductCatalogView>>
{
    public AllProductsProjection()
    {
        State = new List<ProductCatalogView>();
    }

    public void Handle(ProductCreated e)
    {
        State.Add(new ProductCatalogView
        {
            ProductId   = e.ProductId,
            Name        = e.Name,
            Description = e.Description,
            Price       = e.Price,
            Stock       = 0,          // stock arrives via InventoryInitialized
            Sku         = e.Sku,
            SellerId    = e.SellerId,
            SellerName  = e.SellerName,
            LastUpdated = e.Timestamp
        });
    }

    public void Handle(InventoryInitialized e)
    {
        var p = State.FirstOrDefault(p => p.ProductId == e.ProductId);
        if (p is not null)
        {
            p.Stock       = e.InitialQuantity;
            p.LastUpdated = e.Timestamp;
        }
    }

    public void Handle(StockUpdated e)
    {
        var p = State.FirstOrDefault(p => p.ProductId == e.ProductId);
        if (p is not null)
        {
            // Use delta, not e.NewStock : the InventoryAggregate only reads its own stream
            // and is unaware of DCB StockReserved events, so e.NewStock can be stale.
            // The projection already tracks accurate stock via StockReserved, so applying
            // the delta preserves that accuracy.
            p.Stock       = Math.Max(0, p.Stock + e.Delta);
            p.LastUpdated = e.Timestamp;
        }
    }

    public void Handle(StockReserved e)
    {
        var p = State.FirstOrDefault(p => p.ProductId == e.ProductId);
        if (p is not null)
        {
            p.Stock       = Math.Max(0, p.Stock - e.Quantity);
            p.LastUpdated = e.Timestamp;
        }
    }

    public void Handle(PriceUpdated e)
    {
        var p = State.FirstOrDefault(p => p.ProductId == e.ProductId);
        if (p is not null)
        {
            p.Price       = e.NewPrice;
            p.LastUpdated = e.Timestamp;
        }
    }
}

/// <summary>
/// <para>
/// Multi-stream projection that builds one <see cref="OrderFulfillmentView"/> per order,
/// combining events from two different stream types:
/// </para>
/// <list type="bullet">
///   <item>
///     <c>OrderAggregate</c> streams : <see cref="OrderPlaced"/>, <see cref="OrderPaymentProcessed"/>,
///     <see cref="OrderShipped"/>, <see cref="OrderCancelled"/>
///   </item>
///   <item>
///     DCB inventory stream : <see cref="StockReserved"/> (tagged <c>product:{productId}</c>)
///   </item>
/// </list>
/// Endpoint: GET /api/views/fulfillment/{orderId}
/// </summary>
[MultiStreamProjection("OrderFulfillment",
    typeof(OrderPlaced),
    typeof(StockReserved),
    typeof(OrderPaymentProcessed),
    typeof(OrderShipped),
    typeof(OrderCancelled))]
[ProjectionEndpoint(
    route: "/api/views/fulfillment/{orderId}",
    requiredPermission: null,
    cacheMaxAgeSeconds: 2)]
public class OrderFulfillmentProjection
    : ProjectionBase<OrderFulfillmentView>, IMultiStreamEntityResolver
{
    public string? GetEntityId(LawnDart.IEvent @event) => @event switch
    {
        OrderPlaced           e => e.OrderId.ToString(),
        StockReserved         e => e.OrderId.ToString(),
        OrderPaymentProcessed e => e.OrderId.ToString(),
        OrderShipped          e => e.OrderId.ToString(),
        OrderCancelled        e => e.OrderId.ToString(),
        _                       => null
    };

    public void Handle(OrderPlaced e)
    {
        State.OrderId             = e.OrderId;
        State.CustomerId          = e.CustomerId;
        State.CustomerName        = e.CustomerName;
        State.ProductId           = e.ProductId;
        State.ProductName         = e.ProductName;
        State.Quantity            = e.Quantity;
        State.UnitPrice           = e.UnitPrice;
        State.SellerId            = e.SellerId;
        State.SellerName          = e.SellerName;
        State.SellerFulfillmentId = e.SellerFulfillmentId;
        State.PlacedAt            = e.Timestamp;
        State.LastUpdated         = e.Timestamp;
    }

    public void Handle(StockReserved e)
    {
        State.StockReservedAt = e.Timestamp;
        State.LastUpdated     = e.Timestamp;
    }

    public void Handle(OrderPaymentProcessed e)
    {
        State.PaymentReference = e.PaymentReference;
        State.PaidAt           = e.Timestamp;
        State.LastUpdated      = e.Timestamp;
    }

    public void Handle(OrderShipped e)
    {
        State.TrackingNumber = e.TrackingNumber;
        State.ShippedAt      = e.Timestamp;
        State.LastUpdated    = e.Timestamp;
    }

    public void Handle(OrderCancelled e)
    {
        State.CancellationReason = e.Reason;
        State.LastUpdated        = e.Timestamp;
    }
}

/// <summary>
/// DCB-driven per-seller SKU index. Listens to <see cref="SkuReserved"/> events so the
/// read-side always reflects the ground truth from the DCB write-side enforcement.
/// One view instance per seller tenant (stream is scoped per-tenant automatically).
/// Endpoint: GET /api/views/sku-index
/// </summary>
[DcbProjection("ProductSkuIndex", "SkuReserved")]
[ProjectionEndpoint(
    route: "/api/views/sku-index",
    requiredPermission: null,
    cacheMaxAgeSeconds: 5)]
public class ProductSkuIndexProjection : ProjectionBase<ProductSkuIndexView>
{
    public ProductSkuIndexProjection()
    {
        State = new ProductSkuIndexView();
    }

    public void Handle(SkuReserved e)
    {
        var key = $"{e.TenantId}:{e.Sku.ToUpperInvariant()}";
        State.SkuToProductId[key] = e.ProductId;
    }
}
