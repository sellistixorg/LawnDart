using Microsoft.Extensions.Logging;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Demo.ECommerce.Domain.Cart;
using LawnDart.Demo.ECommerce.Domain.Cart.Commands;
using LawnDart.Demo.ECommerce.Domain.Order;
using LawnDart.Demo.ECommerce.Domain.Order.Commands;
using LawnDart.Demo.ECommerce.Projections;
using LawnDart.EventStore;

namespace LawnDart.Demo.ECommerce;

/// <summary>
/// Traditional e-commerce demo using stream-per-aggregate pattern.
/// Demonstrates:
/// - Each aggregate has its own stream (Cart:{id}, Order:{id}, Product:{id})
/// - State reconstructed from stream events
/// - Version-based optimistic concurrency control
/// - Stream-based projections
/// </summary>
public class TraditionalECommerceDemo
{
    private readonly IAggregateRepository _repository;
    private readonly IMetadataProvider _metadataProvider;
    private readonly IEventStore _eventStore;
    private readonly DemoDataSeeder _seeder;
    private readonly ProjectionRunner _projectionRunner;
    private readonly CartViewProjector _cartProjector;
    private readonly OrderViewProjector _orderProjector;
    private readonly ProductCatalogProjector _productCatalogProjector;
    private readonly ILogger<TraditionalECommerceDemo> _logger;

    public TraditionalECommerceDemo(
        IAggregateRepository repository,
        IMetadataProvider metadataProvider,
        IEventStore eventStore,
        DemoDataSeeder seeder,
        ProjectionRunner projectionRunner,
        CartViewProjector cartProjector,
        OrderViewProjector orderProjector,
        ProductCatalogProjector productCatalogProjector,
        ILogger<TraditionalECommerceDemo> logger)
    {
        _repository = repository;
        _metadataProvider = metadataProvider;
        _eventStore = eventStore;
        _seeder = seeder;
        _projectionRunner = projectionRunner;
        _cartProjector = cartProjector;
        _orderProjector = orderProjector;
        _productCatalogProjector = productCatalogProjector;
        _logger = logger;
    }

    public async Task RunAsync()
    {
        Console.WriteLine("\n=== TRADITIONAL APPROACH ===");
        Console.WriteLine("Using: Stream-per-Aggregate pattern");
        Console.WriteLine("- Each aggregate has its own stream (Cart:{id}, Order:{id}, Product:{id})");
        Console.WriteLine("- State reconstructed from stream events");
        Console.WriteLine("- Version-based optimistic concurrency control");
        Console.WriteLine("- Stream-based projections\n");

        // Seed products
        Console.WriteLine("Seeding products...");
        var products = await _seeder.SeedProductsAsync();

        // Manually project product events
        Console.WriteLine("Projecting product events...");
        foreach (var product in products)
        {
            await _projectionRunner.ProjectEventsAsync(product.StreamId);
        }

        // Initialize projections from existing events
        Console.WriteLine("Initializing projections...");
        await _projectionRunner.InitializeAllProjectionsAsync();

        // Display product catalog
        var catalog = await _productCatalogProjector.GetCatalogAsync();
        Console.WriteLine($"\nProduct Catalog ({catalog.Products.Count} products):");
        foreach (var product in catalog.Products.Values)
        {
            Console.WriteLine($"  - {product.Name} ({product.Sku}): ${product.Price:F2} (Stock: {product.Inventory})");
            _cartProjector.SetProductPrice(product.ProductId.ToString(), product.Price);
        }

        // Create a cart
        Console.WriteLine("\n=== Creating Cart ===");
        var cartId = Guid.NewGuid();
        var customerId = "customer-123";
        
        var cart = await _repository.GetOrCreateAsync<Cart>(cartId);

        var createCartCommand = new CreateCartCommand(Guid.NewGuid(), cartId, customerId);
        var commandMetadata = _metadataProvider.CaptureCommandMetadata();
        // TenantId is auto-populated from ITenantContextProvider
        commandMetadata.UserId = customerId;
        commandMetadata.CorrelationId = Guid.NewGuid().ToString();

        // Use HandleCommandAsync for authorization support
        await _repository.HandleCommandAsync(cart, createCartCommand, commandMetadata);

        // Project cart created event
        await _projectionRunner.ProjectEventsAsync(cart.StreamId);

        var cartView = await _cartProjector.GetAsync(cartId.ToString());
        Console.WriteLine($"Cart created:\nCart ID: {cartView?.CartId}\n Customer: {cartView?.CustomerId}\n  Items: {cartView?.ItemCount}\n  Total: ${cartView?.Total:F2}\n  Status: {(cartView?.IsCheckedOut == true ? "Checked Out" : "Active")}\n");

        // Add items to cart
        _logger.LogInformation("=== Adding Items to Cart ===");
        var productIds = products.Select(p => p.State.ProductId.ToString()).Take(3).ToList();

        foreach (var productId in productIds)
        {
            var addItemCommand = new AddItemToCartCommand(Guid.NewGuid(), productId, 2);
            commandMetadata = _metadataProvider.CaptureCommandMetadata();
            // TenantId is auto-populated from ITenantContextProvider
            commandMetadata.UserId = customerId;
            commandMetadata.CorrelationId = Guid.NewGuid().ToString();

            await _repository.HandleCommandAsync(cart, addItemCommand, commandMetadata);

            // Project item added event
            await _projectionRunner.ProjectEventsAsync(cart.StreamId);

            cartView = await _cartProjector.GetAsync(cartId.ToString());
            var product = catalog.Products[productId];
            Console.WriteLine($"\nAdded to cart:\nProduct: {product.Name} ({product.Sku})\n Quantity: 2\n Unit Price: ${product.Price:F2}\n Line Total: ${product.Price * 2:F2}");
            Console.WriteLine($"\nCart Summary:\nTotal Items: {cartView?.ItemCount}\nCart Total: ${cartView?.Total:F2}");
        }

        // Remove an item
        Console.WriteLine("\n=== Removing Item from Cart ===");
        var removeItemCommand = new RemoveItemFromCartCommand(Guid.NewGuid(), productIds[0]);
        commandMetadata = _metadataProvider.CaptureCommandMetadata();
        // TenantId is auto-populated from ITenantContextProvider
        commandMetadata.UserId = customerId;
        commandMetadata.CorrelationId = Guid.NewGuid().ToString();

        await _repository.HandleCommandAsync(cart, removeItemCommand, commandMetadata);

        // Project item removed event
        await _projectionRunner.ProjectEventsAsync(cart.StreamId);

        cartView = await _cartProjector.GetAsync(cartId.ToString());
        var removedProduct = catalog.Products[productIds[0]];
        Console.WriteLine($"\nRemoved from cart:\nProduct: {removedProduct.Name} ({removedProduct.Sku})");
        Console.WriteLine($"\nCart Summary:\nTotal Items: {cartView?.ItemCount} \nCart Total: ${cartView?.Total:F2}");
        Console.WriteLine($"\nCart Contents:");
        if (cartView?.Items != null)
        {
            foreach (var item in cartView.Items)
            {
                var itemProduct = catalog.Products[item.ProductId];
                Console.WriteLine($"  - {itemProduct.Name} ({itemProduct.Sku}): {item.Quantity} x ${itemProduct.Price:F2} = ${item.Quantity * itemProduct.Price:F2}");
            }
        }

        // Checkout cart
        Console.WriteLine("\n=== Checking Out Cart ===");
        var checkoutCommand = new CheckoutCartCommand(Guid.NewGuid());
        commandMetadata = _metadataProvider.CaptureCommandMetadata();
        // TenantId is auto-populated from ITenantContextProvider
        commandMetadata.UserId = customerId;
        commandMetadata.CorrelationId = Guid.NewGuid().ToString();

        await _repository.HandleCommandAsync(cart, checkoutCommand, commandMetadata);

        // Project cart checked out event
        await _projectionRunner.ProjectEventsAsync(cart.StreamId);

        cartView = await _cartProjector.GetAsync(cartId.ToString());
        Console.WriteLine($"\nCart checked out:");
        Console.WriteLine($"\nOrder ID: {cartView?.OrderId}\nFinal Total: ${cartView?.Total:F2}");
        Console.WriteLine($"\nCart Contents:");
        if (cartView?.Items != null)
        {
            foreach (var item in cartView.Items)
            {
                var itemProduct = catalog.Products[item.ProductId];
                Console.WriteLine($"  - {itemProduct.Name} ({itemProduct.Sku}): {item.Quantity} x ${itemProduct.Price:F2} = ${item.Quantity * itemProduct.Price:F2}");
            }
        }

        // Create order from cart
        Console.WriteLine("\n=== Creating Order ===");
        var orderId = cartView?.OrderId ?? Guid.NewGuid();
        
        var order = await _repository.GetOrCreateAsync<Order>(orderId);

        var createOrderCommand = new CreateOrderCommand(Guid.NewGuid(), orderId, cartId, customerId);
        commandMetadata = _metadataProvider.CaptureCommandMetadata();
        // TenantId is auto-populated from ITenantContextProvider
        commandMetadata.UserId = customerId;
        commandMetadata.CorrelationId = Guid.NewGuid().ToString();

        await _repository.HandleCommandAsync(order, createOrderCommand, commandMetadata);

        // Add order items from cart - use HandleCommandAsync for consistency
        if (cartView?.Items != null)
        {
            foreach (var item in cartView.Items)
            {
                var product = catalog.Products[item.ProductId];
                var addOrderItemCommand = new AddOrderItemCommand(
                    Guid.NewGuid(),
                    item.ProductId,
                    item.Quantity,
                    product.Price);

                commandMetadata = _metadataProvider.CaptureCommandMetadata();
                // TenantId is auto-populated from ITenantContextProvider
                commandMetadata.UserId = customerId;
                commandMetadata.CorrelationId = Guid.NewGuid().ToString();

                await _repository.HandleCommandAsync(order, addOrderItemCommand, commandMetadata);
            }
        }

        // Project order created event
        await _projectionRunner.ProjectEventsAsync(order.StreamId);

        var orderView = await _orderProjector.GetAsync(orderId.ToString());
        Console.WriteLine($"\nOrder created:\nOrder ID: {orderView?.OrderId}\nCustomer: {orderView?.CustomerId}\nStatus: {(orderView?.IsCompleted == true ? "Completed" : "Pending")}");
        Console.WriteLine($"\nOrder Items:");
        if (orderView?.Items != null)
        {
            foreach (var item in orderView.Items)
            {
                var itemProduct = catalog.Products[item.ProductId];
                Console.WriteLine($"  - {itemProduct.Name} ({itemProduct.Sku}):\nQuantity: {item.Quantity}\nUnit Price: ${item.UnitPrice:F2}\nLine Total: ${item.LineTotal:F2}");
            }
        }
        Console.WriteLine($"\nOrder Total: ${orderView?.Total:F2}");

        // Complete order
        Console.WriteLine("\n=== Completing Order ===");
        var completeOrderCommand = new CompleteOrderCommand(Guid.NewGuid());
        commandMetadata = _metadataProvider.CaptureCommandMetadata();
        // TenantId is auto-populated from ITenantContextProvider
        commandMetadata.UserId = customerId;
        commandMetadata.CorrelationId = Guid.NewGuid().ToString();

        await _repository.HandleCommandAsync(order, completeOrderCommand, commandMetadata);

        // Project order completed event
        await _projectionRunner.ProjectEventsAsync(order.StreamId);

        orderView = await _orderProjector.GetAsync(orderId.ToString());
        Console.WriteLine($"\nOrder completed:\nOrder ID: {orderView?.OrderId}\nStatus: {(orderView?.IsCompleted == true ? "Completed" : "Pending")}\nTotal: ${orderView?.Total:F2}");

        // Display stream registry info
        _logger.LogInformation("\n=== Stream Registry ===");
        var cartStreams = await _eventStore.GetStreamsByAggregateTypeAsync("Cart");
        var orderStreams = await _eventStore.GetStreamsByAggregateTypeAsync("Order");
        var productStreams = await _eventStore.GetStreamsByAggregateTypeAsync("Product");

        Console.WriteLine($"\nStream Registry Summary:");
        Console.WriteLine($"Cart streams: {cartStreams.Count}");
        Console.WriteLine($"Order streams: {orderStreams.Count}");
        Console.WriteLine($"Product streams: {productStreams.Count}");
        Console.WriteLine($"Stream ids are {{tenant}}:Cart:{{id}}, {{tenant}}:Order:{{id}}, and {{tenant}}:Product:{{id}}.");
        if (cartView?.IsCheckedOut != true || cartView.OrderId is null)
            throw new InvalidOperationException("Cart did not check out.");
        if (orderView?.IsCompleted != true)
            throw new InvalidOperationException("Order did not complete.");
        if (cartStreams.Count < 1 || orderStreams.Count < 1 || productStreams.Count < 5)
            throw new InvalidOperationException(
                $"Expected cart, order, and product streams. Got {cartStreams.Count}, {orderStreams.Count}, {productStreams.Count}.");
    }
}

