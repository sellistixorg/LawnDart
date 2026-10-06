using Microsoft.Extensions.Logging;
using LawnDart;
using LawnDart.Demo.ECommerce;
using LawnDart.Demo.ECommerce.Domain.Cart;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Demo.ECommerce.Dcb;

/// <summary>
/// DCB e-commerce demo using Dynamic Consistency Boundaries pattern.
/// Demonstrates:
/// - Events tagged with entity ids (cart:{id}, order:{id}, product:{id})
/// - Each append is its own write. The store assigns the stream id.
/// - Tag-based queries for state reconstruction
/// - Sequence position-based optimistic concurrency control
/// - Cross-entity queries
/// </summary>
public class DcbECommerceDemo
{
    private readonly IEventStore _eventStore;
    private readonly IMetadataProvider _metadataProvider;
    private readonly DcbDemoDataSeeder _seeder;
    private readonly DcbCartService _cartService;
    private readonly DcbOrderService _orderService;
    private readonly DcbProductService _productService;
    private readonly DcbCartViewProjector _cartProjector;
    private readonly DcbOrderViewProjector _orderProjector;
    private readonly DcbProductCatalogProjector _productCatalogProjector;
    private readonly ILogger<DcbECommerceDemo> _logger;

    public DcbECommerceDemo(
        IEventStore eventStore,
        IMetadataProvider metadataProvider,
        DcbDemoDataSeeder seeder,
        DcbCartService cartService,
        DcbOrderService orderService,
        DcbProductService productService,
        DcbCartViewProjector cartProjector,
        DcbOrderViewProjector orderProjector,
        DcbProductCatalogProjector productCatalogProjector,
        ILogger<DcbECommerceDemo> logger)
    {
        _eventStore = eventStore;
        _metadataProvider = metadataProvider;
        _seeder = seeder;
        _cartService = cartService;
        _orderService = orderService;
        _productService = productService;
        _cartProjector = cartProjector;
        _orderProjector = orderProjector;
        _productCatalogProjector = productCatalogProjector;
        _logger = logger;
    }

    public async Task RunAsync()
    {
        Console.WriteLine("\n=== DCB APPROACH ===");
        Console.WriteLine("Using: Dynamic Consistency Boundaries");
        Console.WriteLine("- Each append is its own write. Queries use tags.");
        Console.WriteLine("- Events tagged with entity IDs (cart:{id}, order:{id}, product:{id})");
        Console.WriteLine("- Tag-based queries for state reconstruction");
        Console.WriteLine("- Sequence position-based optimistic concurrency control");
        Console.WriteLine("- Cross-entity queries\n");

        // Seed products
        Console.WriteLine("Seeding products...");
        var productIds = await _seeder.SeedProductsAsync();

        // Build product catalog
        var catalog = await _productCatalogProjector.GetCatalogAsync();
        Console.WriteLine($"\nProduct Catalog ({catalog.Products.Count} products):");
        foreach (var product in catalog.Products.Values)
        {
            Console.WriteLine($"  - {product.Name} ({product.Sku}): ${product.Price:F2} (Stock: {product.Inventory})");
            _cartProjector.SetProductPrice(product.ProductId.ToString(), product.Price);
        }

        // Create a cart
        Console.WriteLine("\n=== Creating Cart ===");
        var tenantId = "tenant-123";
        var cartId = Guid.NewGuid();
        var customerId = "customer-123";
        
        var commandMetadata = _metadataProvider.CaptureCommandMetadata();
        commandMetadata.TenantId = tenantId;
        commandMetadata.UserId = customerId;
        commandMetadata.CorrelationId = Guid.NewGuid().ToString();

        await _cartService.CreateCartAsync(cartId, customerId);

        var cartView = await _cartProjector.GetAsync(cartId);
        Console.WriteLine($"Cart created:\nCart ID: {cartView?.CartId}\n Customer: {cartView?.CustomerId}\n  Items: {cartView?.ItemCount}\n  Total: ${cartView?.Total:F2}\n  Status: {(cartView?.IsCheckedOut == true ? "Checked Out" : "Active")}\n");

        // Add items to cart
        _logger.LogInformation("=== Adding Items to Cart ===");
        var productIdStrings = productIds.Take(3).Select(id => id.ToString()).ToList();

        foreach (var productId in productIdStrings)
        {
            commandMetadata = _metadataProvider.CaptureCommandMetadata();
            commandMetadata.TenantId = tenantId;
            commandMetadata.UserId = customerId;
            commandMetadata.CorrelationId = Guid.NewGuid().ToString();

            await _cartService.AddItemAsync(cartId, productId, 2);

            cartView = await _cartProjector.GetAsync(cartId);
            var product = catalog.Products[productId];
            Console.WriteLine($"\nAdded to cart:\nProduct: {product.Name} ({product.Sku})\n Quantity: 2\n Unit Price: ${product.Price:F2}\n Line Total: ${product.Price * 2:F2}");
            Console.WriteLine($"\nCart Summary:\nTotal Items: {cartView?.ItemCount}\nCart Total: ${cartView?.Total:F2}");
        }

        // Remove an item
        Console.WriteLine("\n=== Removing Item from Cart ===");
        commandMetadata = _metadataProvider.CaptureCommandMetadata();
        commandMetadata.TenantId = tenantId;
        commandMetadata.UserId = customerId;
        commandMetadata.CorrelationId = Guid.NewGuid().ToString();

        await _cartService.RemoveItemAsync(cartId, productIdStrings[0]);

        cartView = await _cartProjector.GetAsync(cartId);
        var removedProduct = catalog.Products[productIdStrings[0]];
        Console.WriteLine($"\nRemoved from cart:\nProduct: {removedProduct.Name} ({removedProduct.Sku})");
        Console.WriteLine($"\nCart Summary:\nTotal Items: {cartView?.ItemCount} \nCart Total: ${cartView?.Total:F2}");
        Console.WriteLine($"\nCart Contents:");
        foreach (var item in cartView!.Items)
        {
            var itemProduct = catalog.Products[item.ProductId];
            Console.WriteLine($"  - {itemProduct.Name} ({itemProduct.Sku}): {item.Quantity} x ${itemProduct.Price:F2} = ${item.Quantity * itemProduct.Price:F2}");
        }

        // Checkout cart
        Console.WriteLine("\n=== Checking Out Cart ===");
        commandMetadata = _metadataProvider.CaptureCommandMetadata();
        commandMetadata.TenantId = tenantId;
        commandMetadata.UserId = customerId;
        commandMetadata.CorrelationId = Guid.NewGuid().ToString();

        await _cartService.CheckoutCartAsync(cartId);

        cartView = await _cartProjector.GetAsync(cartId);
        if (cartView?.OrderId == null)
        {
            throw new InvalidOperationException("Cart checkout did not generate an order ID");
        }
        var orderId = cartView.OrderId.Value;
        Console.WriteLine($"\nCart checked out:");
        Console.WriteLine($"\nOrder ID: {orderId}\nFinal Total: ${cartView.Total:F2}");
        Console.WriteLine($"\nCart Contents:");
        foreach (var item in cartView.Items)
        {
            var itemProduct = catalog.Products[item.ProductId];
            Console.WriteLine($"  - {itemProduct.Name} ({itemProduct.Sku}): {item.Quantity} x ${itemProduct.Price:F2} = ${item.Quantity * itemProduct.Price:F2}");
        }

        // Create order from cart
        Console.WriteLine("\n=== Creating Order ===");
        commandMetadata = _metadataProvider.CaptureCommandMetadata();
        commandMetadata.TenantId = tenantId;
        commandMetadata.UserId = customerId;
        commandMetadata.CorrelationId = Guid.NewGuid().ToString();

        await _orderService.CreateOrderAsync(orderId, cartId, customerId);

        // Add order items from cart
        foreach (var item in cartView.Items)
        {
            var product = catalog.Products[item.ProductId];
            await _orderService.AddOrderItemAsync(orderId, item.ProductId, item.Quantity, product.Price);
        }

        var orderView = await _orderProjector.GetAsync(orderId);
        Console.WriteLine($"\nOrder created:\nOrder ID: {orderView?.OrderId}\nCustomer: {orderView?.CustomerId}\nStatus: {(orderView?.IsCompleted == true ? "Completed" : "Pending")}");
        Console.WriteLine($"\nOrder Items:");
        foreach (var item in orderView!.Items)
        {
            var itemProduct = catalog.Products[item.ProductId];
            Console.WriteLine($"  - {itemProduct.Name} ({itemProduct.Sku}):\nQuantity: {item.Quantity}\nUnit Price: ${item.UnitPrice:F2}\nLine Total: ${item.LineTotal:F2}");
        }
        Console.WriteLine($"\nOrder Total: ${orderView.Total:F2}");

        // Complete order
        Console.WriteLine("\n=== Completing Order ===");
        commandMetadata = _metadataProvider.CaptureCommandMetadata();
        commandMetadata.TenantId = tenantId;
        commandMetadata.UserId = customerId;
        commandMetadata.CorrelationId = Guid.NewGuid().ToString();

        await _orderService.CompleteOrderAsync(orderId);

        orderView = await _orderProjector.GetAsync(orderId);
        Console.WriteLine($"\nOrder completed:\nOrder ID: {orderView?.OrderId}\nStatus: {(orderView?.IsCompleted == true ? "Completed" : "Pending")}\nTotal: ${orderView?.Total:F2}");

        // Demonstrate cross-entity query
        Console.WriteLine("\n=== Cross-Entity Query Demo ===");
        Console.WriteLine("Querying all events related to the order using tags...");
        var orderQuery = Query.FromItems(QueryItem.ByTags($"order:{orderId}"));
        var orderEventsRaw = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(orderQuery))
            orderEventsRaw.Add(evt);
        Console.WriteLine($"Found {orderEventsRaw.Count} events tagged with order:{orderId}");

        var cartQuery = Query.FromItems(QueryItem.ByTags($"cart:{cartId}"));
        var cartEventsRaw = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(cartQuery))
            cartEventsRaw.Add(evt);
        Console.WriteLine($"Found {cartEventsRaw.Count} events tagged with cart:{cartId}");

        Console.WriteLine();
        Console.WriteLine("Note: each DCB append is its own write. The store assigns the stream id.");
        Console.WriteLine("Tags (cart:{id}, order:{id}) are how these queries find the events.");
        if (orderView?.IsCompleted != true)
            throw new InvalidOperationException("DCB order did not complete.");
        if (orderEventsRaw.Count == 0 || cartEventsRaw.Count == 0)
            throw new InvalidOperationException("DCB tag queries returned no events.");

        var workflow = new DcbOrderFulfillmentWorkflow(_eventStore, _metadataProvider);
        var productForWorkflow = productIds[0].ToString();
        await workflow.ExecuteWorkflowAsync(Guid.NewGuid(), Guid.NewGuid(), productForWorkflow, 1);
        await workflow.DemonstrateConcurrencyConflictAsync(Guid.NewGuid(), Guid.NewGuid(), productForWorkflow);
    }
}

