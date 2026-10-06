using Microsoft.Extensions.Logging;
using LawnDart.Dcb;
using LawnDart.Demo.ECommerce.Domain.Order.Commands;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Demo.ECommerce.Dcb;

/// <summary>
/// Enhanced DCB demo using the new IDcbRepository and DcbEntity patterns.
/// Demonstrates the simplified approach with automatic metadata and tag handling.
/// </summary>
public class EnhancedDcbDemo
{
    private readonly IDcbRepository _dcbRepository;
    private readonly IMetadataProvider _metadataProvider;
    private readonly IEventStore _eventStore;
    private readonly ILogger<EnhancedDcbDemo> _logger;

    public EnhancedDcbDemo(
        IDcbRepository dcbRepository,
        IMetadataProvider metadataProvider,
        IEventStore eventStore,
        ILogger<EnhancedDcbDemo> logger)
    {
        _dcbRepository = dcbRepository;
        _metadataProvider = metadataProvider;
        _eventStore = eventStore;
        _logger = logger;
    }

    public async Task RunAsync()
    {
        Console.WriteLine("\n+===============================================================+");
        Console.WriteLine("|            ENHANCED DCB PATTERN DEMONSTRATION                  |");
        Console.WriteLine("+===============================================================+\n");

        Console.WriteLine("This demo shows the new DCB patterns:");
        Console.WriteLine("  * IDcbRepository - High-level repository for DCB operations");
        Console.WriteLine("  * DcbEntity<TState> - Base class with Emit() and command handling");
        Console.WriteLine("  * Automatic metadata enrichment");
        Console.WriteLine("  * Authorization integration");
        Console.WriteLine("  * Tag-based querying\n");

        var orderId = Guid.NewGuid();
        var customerId = "customer-dcb-123";
        var cartId = Guid.NewGuid();

        await CreateOrderWithEntity(orderId, cartId, customerId);
        await AddItemsToOrder(orderId);
        await CompleteOrder(orderId);
        await QueryOrderByTags(orderId);
        await ShowComparison();
    }

    private async Task CreateOrderWithEntity(Guid orderId, Guid cartId, string customerId)
    {
        Console.WriteLine("=== Step 1: Create Order Using DcbEntity ===");

        // Create entity with tags
        var entity = await _dcbRepository.CreateEntityAsync<DcbOrderEntity>(
            new[] { $"order:{orderId}" });

        Console.WriteLine($"  [ok] Created DcbOrderEntity with tags: order:{orderId}");

        // Handle command
        var command = new CreateOrderCommand(Guid.NewGuid(), orderId, cartId, customerId);
        var metadata = _metadataProvider.CaptureCommandMetadata();
        metadata.UserId = customerId;
        metadata.TenantId = "demo-tenant";

        // Repository handles command, authorization, and metadata automatically
        await _dcbRepository.HandleCommandAsync(entity, command, metadata);

        Console.WriteLine($"  [ok] Order created: {orderId}");
        Console.WriteLine($"    Automatically tagged with:");
        Console.WriteLine($"      - order:{orderId}");
        Console.WriteLine($"      - cart:{cartId}");
        Console.WriteLine($"      - customer:{customerId}");
        Console.WriteLine($"    Authorization checked (if configured)");
        Console.WriteLine($"    Metadata enriched automatically");
        Console.WriteLine();
    }

    private async Task AddItemsToOrder(Guid orderId)
    {
        Console.WriteLine("=== Step 2: Add Items to Order ===");

        // Load existing entity
        var entity = await _dcbRepository.GetOrCreateEntityAsync<DcbOrderEntity>(
            new[] { $"order:{orderId}" });

        Console.WriteLine($"  [ok] Loaded order entity (rebuilt from tags)");

        // Add multiple items
        var products = new[]
        {
            ("product-001", 2, 19.99m),
            ("product-002", 1, 49.99m),
            ("product-003", 3, 9.99m)
        };

        foreach (var (productId, quantity, price) in products)
        {
            var command = new AddOrderItemCommand(Guid.NewGuid(), productId, quantity, price);
            var metadata = _metadataProvider.CaptureCommandMetadata();

            await _dcbRepository.HandleCommandAsync(entity, command, metadata);
            
            Console.WriteLine($"  [ok] Added {quantity}x {productId} @ ${price:F2}");
        }

        Console.WriteLine($"\n  Order Total: ${entity.State.Total:F2}");
        Console.WriteLine($"  Total Items: {entity.State.Items.Count}");
        Console.WriteLine();
    }

    private async Task CompleteOrder(Guid orderId)
    {
        Console.WriteLine("=== Step 3: Complete Order ===");

        var entity = await _dcbRepository.GetOrCreateEntityAsync<DcbOrderEntity>(
            new[] { $"order:{orderId}" });

        var command = new CompleteOrderCommand(Guid.NewGuid());
        var metadata = _metadataProvider.CaptureCommandMetadata();

        await _dcbRepository.HandleCommandAsync(entity, command, metadata);

        Console.WriteLine($"  [ok] Order completed");
        Console.WriteLine($"  Status: {(entity.State.IsCompleted ? "Completed" : "Pending")}");
        Console.WriteLine($"  Final Total: ${entity.State.Total:F2}");
        Console.WriteLine();
    }

    private async Task QueryOrderByTags(Guid orderId)
    {
        Console.WriteLine("=== Step 4: Query Order by Tags ===");

        // Method 1: Get state directly
        var state = await _dcbRepository.GetStateAsync<Domain.Order.OrderState>(
            new[] { $"order:{orderId}" });

        Console.WriteLine($"  Using GetStateAsync<OrderState>:");
        Console.WriteLine($"    Order ID: {state.OrderId}");
        Console.WriteLine($"    Customer: {state.CustomerId}");
        Console.WriteLine($"    Items: {state.Items.Count}");
        Console.WriteLine($"    Total: ${state.Total:F2}");
        Console.WriteLine($"    Status: {(state.IsCompleted ? "Completed" : "Pending")}");

        // Method 2: Query by customer
        Console.WriteLine($"\n  Query all orders for customer:");
        var query = Query.FromItems(QueryItem.ByTags($"customer:{state.CustomerId}"));
        var customerEvents = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(query))
            customerEvents.Add(evt);
        
        var orderIds = customerEvents
            .Select(e => e.Tags.FirstOrDefault(t => t.StartsWith("order:")))
            .Where(t => t != null)
            .Distinct()
            .ToList();

        Console.WriteLine($"    Found {orderIds.Count} order(s) for customer {state.CustomerId}");
        foreach (var orderTag in orderIds)
        {
            Console.WriteLine($"      - {orderTag}");
        }

        Console.WriteLine();
    }

    private async Task ShowComparison()
    {
        Console.WriteLine("=== Traditional vs DCB Approach ===\n");

        Console.WriteLine("TRADITIONAL (Stream-per-Aggregate):");
        Console.WriteLine("  * Each order has its own stream: 'Order:{id}'");
        Console.WriteLine("  * Load order: Read stream 'Order:{id}'");
        Console.WriteLine("  * Version-based concurrency");
        Console.WriteLine("  * Find customer orders: Query stream registry\n");

        Console.WriteLine("DCB (Tag-based):");
        Console.WriteLine("  * No streams - just events with tags");
        Console.WriteLine("  * Load order: Query by tag 'order:{id}'");
        Console.WriteLine("  * Sequence-based concurrency with AppendCondition");
        Console.WriteLine("  * Find customer orders: Query by tag 'customer:{id}'");
        Console.WriteLine("  * Can query by ANY tag combination!\n");

        Console.WriteLine("Benefits of DCB:");
        Console.WriteLine("  [ok] Flexible querying (any tag combination)");
        Console.WriteLine("  [ok] Cross-aggregate coordination");
        Console.WriteLine("  [ok] No fixed aggregate boundaries");
        Console.WriteLine("  [ok] Better for complex workflows");
        Console.WriteLine("  [ok] DcbEntity<TState> provides clean encapsulation\n");

        Console.WriteLine("When to use DCB:");
        Console.WriteLine("  * Complex workflows spanning multiple entities");
        Console.WriteLine("  * Need to query events multiple ways");
        Console.WriteLine("  * Dynamic consistency boundaries");
        Console.WriteLine("  * Sagas and process managers\n");

        await Task.CompletedTask;
    }
}
