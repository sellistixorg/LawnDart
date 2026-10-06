using Microsoft.Extensions.Logging;
using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.Demo.ECommerce.Domain.Product;
using LawnDart.Demo.ECommerce.Domain.Product.Commands;
using LawnDart.Demo.ECommerce.Domain.Order.Commands;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Demo.ECommerce;

/// <summary>
/// Hybrid demo showing BOTH traditional Aggregate and DCB patterns in the same application.
/// Demonstrates:
/// - Traditional Aggregates for simple CRUD entities (Product catalog)
/// - DCB for complex workflows (Multi-warehouse inventory + Order fulfillment)
/// - How to choose between approaches
/// - How both patterns coexist and complement each other
/// </summary>
public class HybridPatternDemo
{
    private readonly IAggregateRepository _aggregateRepository;
    private readonly IDcbRepository _dcbRepository;
    private readonly IMetadataProvider _metadataProvider;
    private readonly IEventStore _eventStore;
    private readonly ILogger<HybridPatternDemo> _logger;

    public HybridPatternDemo(
        IAggregateRepository aggregateRepository,
        IDcbRepository dcbRepository,
        IMetadataProvider metadataProvider,
        IEventStore eventStore,
        ILogger<HybridPatternDemo> logger)
    {
        _aggregateRepository = aggregateRepository;
        _dcbRepository = dcbRepository;
        _metadataProvider = metadataProvider;
        _eventStore = eventStore;
        _logger = logger;
    }

    public async Task RunAsync()
    {
        Console.WriteLine("\n+===============================================================+");
        Console.WriteLine("|          HYBRID PATTERN DEMONSTRATION                          |");
        Console.WriteLine("|     Traditional Aggregates + DCB Working Together              |");
        Console.WriteLine("+===============================================================+\n");

        Console.WriteLine("This demo shows how BOTH patterns work together in one application:\n");
        Console.WriteLine("  TRADITIONAL AGGREGATES:");
        Console.WriteLine("   Used for: Product Catalog (simple CRUD)");
        Console.WriteLine("   Why: Clear boundaries, simple lifecycle, no cross-entity logic\n");
        
        Console.WriteLine("  DCB PATTERN:");
        Console.WriteLine("   Used for: Multi-Warehouse Inventory + Order Fulfillment");
        Console.WriteLine("   Why: Complex workflows, cross-entity coordination, flexible queries\n");

        await ShowDecisionFramework();
        await Part1_TraditionalProducts();
        await Part2_DcbInventoryWorkflow();
        await Part3_DcbOrderFulfillment();
        await Part4_QueryComparison();
        await ShowArchitecturalGuidance();
    }

    private async Task ShowDecisionFramework()
    {
        Console.WriteLine("===============================================================");
        Console.WriteLine("DECISION FRAMEWORK: When to use which pattern?");
        Console.WriteLine("===============================================================\n");

        Console.WriteLine("[ok] Use TRADITIONAL AGGREGATES when:");
        Console.WriteLine("   * Clear, stable aggregate boundaries");
        Console.WriteLine("   * Simple CRUD operations");
        Console.WriteLine("   * Single entity lifecycle (create -> update -> delete)");
        Console.WriteLine("   * No cross-aggregate coordination needed");
        Console.WriteLine("   * Version-based concurrency is sufficient");
        Console.WriteLine("   Examples: User profiles, Product catalog, Customer accounts\n");

        Console.WriteLine("[ok] Use DCB PATTERN when:");
        Console.WriteLine("   * Complex workflows spanning multiple entities");
        Console.WriteLine("   * Need flexible querying by multiple dimensions");
        Console.WriteLine("   * Cross-entity coordination and consistency");
        Console.WriteLine("   * Dynamic consistency boundaries");
        Console.WriteLine("   * Saga or process manager scenarios");
        Console.WriteLine("   Examples: Order fulfillment, Inventory allocation, Payment processing\n");

        Console.WriteLine("  Both patterns can coexist in the same application!");
        Console.WriteLine("   Choose based on the specific needs of each subdomain.\n");
        
        await Task.CompletedTask;
    }

    private async Task Part1_TraditionalProducts()
    {
        Console.WriteLine("\n===============================================================");
        Console.WriteLine("PART 1: Traditional Aggregates - Product Catalog");
        Console.WriteLine("===============================================================\n");

        Console.WriteLine("Managing products with traditional stream-per-aggregate:\n");

        // Create three products using traditional aggregates
        var productIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        var products = new[]
        {
            ("Laptop", "LAP-001", 999.99m, 50),
            ("Mouse", "MOU-001", 29.99m, 200),
            ("Keyboard", "KEY-001", 79.99m, 150)
        };

        for (int i = 0; i < products.Length; i++)
        {
            var (name, sku, price, stock) = products[i];
            var productId = productIds[i];

            var product = await _aggregateRepository.GetOrCreateAsync<Product>(productId);
            
            var createCommand = new CreateProductCommand(Guid.NewGuid(), productId, name, sku, price, stock);
            var metadata = _metadataProvider.CaptureCommandMetadata();
            metadata.TenantId = "demo-tenant";
            metadata.UserId = "admin-user";

            await _aggregateRepository.HandleCommandAsync(product, createCommand, metadata);

            Console.WriteLine($"[ok] Created Product Aggregate:");
            Console.WriteLine($"   Stream: {product.StreamId}");
            Console.WriteLine($"   Name: {name} ({sku})");
            Console.WriteLine($"   Price: ${price:F2}, Stock: {stock}");
            Console.WriteLine($"   Pattern: Stream-per-aggregate");
            Console.WriteLine($"   Concurrency: Version-based\n");
        }

        Console.WriteLine("Why Traditional Aggregates here?");
        Console.WriteLine("* Products have clear boundaries (each product is independent)");
        Console.WriteLine("* Simple lifecycle: create -> update -> archive");
        Console.WriteLine("* No cross-product coordination needed");
        Console.WriteLine("* Easy to load: GetOrCreateAsync uses {tenant}:Product:{id}");
        Console.WriteLine("* Standard version-based concurrency works perfectly\n");

        // Store for later use
        _productIds = productIds;
    }

    private Guid[] _productIds = Array.Empty<Guid>();

    private async Task Part2_DcbInventoryWorkflow()
    {
        Console.WriteLine("\n===============================================================");
        Console.WriteLine("PART 2: DCB Pattern - Multi-Warehouse Inventory");
        Console.WriteLine("===============================================================\n");

        Console.WriteLine("Managing inventory across warehouses with DCB:\n");

        var productId = _productIds[0]; // Laptop
        var warehouseIds = new[] { "WH-WEST", "WH-EAST", "WH-CENTRAL" };
        var stockLevels = new[] { 20, 15, 15 }; // Total: 50 (matches product stock)

        // Allocate inventory to warehouses using DCB (no aggregate boundaries!)
        for (int i = 0; i < warehouseIds.Length; i++)
        {
            var warehouseId = warehouseIds[i];
            var stock = stockLevels[i];

            var inventoryEvent = new InventoryAllocatedEvent
            {
                Id = Guid.NewGuid(),
                Timestamp = DateTime.UtcNow,
                ProductId = productId.ToString(),
                WarehouseId = warehouseId,
                Quantity = stock
            };

            var tags = new[]
            {
                $"product:{productId}",
                $"warehouse:{warehouseId}",
                $"inventory-allocation"
            };

            await _dcbRepository.AppendWithContextAsync(
                new IEvent[] { inventoryEvent },
                tags);

            Console.WriteLine($"[ok] Allocated Inventory (DCB):");
            Console.WriteLine($"   Product: {productId}");
            Console.WriteLine($"   Warehouse: {warehouseId}");
            Console.WriteLine($"   Quantity: {stock}");
            Console.WriteLine($"   Tags: {string.Join(", ", tags)}");
            Console.WriteLine("   Pattern: tagged events. The store assigns the stream id.");
            Console.WriteLine($"   Concurrency: Sequence-based with AppendCondition\n");
        }

        Console.WriteLine("Why DCB Pattern here?");
        Console.WriteLine("* Need to query inventory by product OR by warehouse");
        Console.WriteLine("* Cross-warehouse transfers require coordination");
        Console.WriteLine("* No single 'Inventory' aggregate - it's distributed");
        Console.WriteLine("* Can query: 'Show all inventory for warehouse:WH-WEST'");
        Console.WriteLine("* Can query: 'Show all warehouses with product:{id}'");
        Console.WriteLine("* AppendCondition prevents double-allocation\n");

        // Demonstrate cross-warehouse transfer using DCB coordination
        Console.WriteLine("--- Cross-Warehouse Transfer (DCB Coordination) ---\n");

        var transferQuantity = 5;
        var fromWarehouse = "WH-WEST";
        var toWarehouse = "WH-EAST";

        // One append writes the withdrawal and the deposit.
        var transferId = Guid.NewGuid();
        var events = new IEvent[]
        {
            new InventoryTransferredEvent
            {
                Id = Guid.NewGuid(),
                Timestamp = DateTime.UtcNow,
                TransferId = transferId,
                ProductId = productId.ToString(),
                FromWarehouse = fromWarehouse,
                ToWarehouse = toWarehouse,
                Quantity = transferQuantity,
                Direction = "OUT"
            },
            new InventoryTransferredEvent
            {
                Id = Guid.NewGuid(),
                Timestamp = DateTime.UtcNow,
                TransferId = transferId,
                ProductId = productId.ToString(),
                FromWarehouse = fromWarehouse,
                ToWarehouse = toWarehouse,
                Quantity = transferQuantity,
                Direction = "IN"
            }
        };

        var transferTags = new[]
        {
            $"product:{productId}",
            $"warehouse:{fromWarehouse}",
            $"warehouse:{toWarehouse}",
            $"transfer:{transferId}"
        };

        // One append. A later write is a separate append.
        // An AppendCondition would fence a concurrent transfer of the same stock.
        await _dcbRepository.AppendWithContextAsync(events, transferTags);

        Console.WriteLine($"[ok] Transferred Inventory:");
        Console.WriteLine($"   Transfer ID: {transferId}");
        Console.WriteLine($"   Product: {productId}");
        Console.WriteLine($"   From: {fromWarehouse} -> To: {toWarehouse}");
        Console.WriteLine($"   Quantity: {transferQuantity}");
        Console.WriteLine($"   Coordination: one append writes both events");
        Console.WriteLine($"     This would be complex with traditional aggregates");
        Console.WriteLine($"     In production: Use AppendCondition to prevent race conditions\n");
    }

    private async Task Part3_DcbOrderFulfillment()
    {
        Console.WriteLine("\n===============================================================");
        Console.WriteLine("PART 3: DCB Pattern - Order Fulfillment Workflow");
        Console.WriteLine("===============================================================\n");

        Console.WriteLine("Managing order fulfillment with DCB:\n");

        var orderId = Guid.NewGuid();
        var customerId = "customer-123";
        var productId = _productIds[0];
        var warehouseId = "WH-WEST";
        var quantity = 2;

        // Step 1: Create Order (DCB style)
        var orderCreatedEvent = new OrderCreatedEvent
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            OrderId = orderId,
            CustomerId = customerId,
            ProductId = productId.ToString(),
            Quantity = quantity
        };

        var orderTags = new[]
        {
            $"order:{orderId}",
            $"customer:{customerId}",
            $"product:{productId}",
            "workflow:order-fulfillment"
        };

        await _dcbRepository.AppendWithContextAsync(
            new IEvent[] { orderCreatedEvent },
            orderTags);

        Console.WriteLine($"[ok] Order Created (DCB):");
        Console.WriteLine($"   Order ID: {orderId}");
        Console.WriteLine($"   Customer: {customerId}");
        Console.WriteLine($"   Product: {productId}");
        Console.WriteLine($"   Quantity: {quantity}");
        Console.WriteLine($"   Tags: {string.Join(", ", orderTags)}\n");

        // Step 2: Reserve Inventory from Warehouse
        var workflowLastPos = await _dcbRepository.GetLastSequencePositionAsync(
            new[] { $"order:{orderId}" });

        var reservationEvent = new InventoryReservedEvent
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            OrderId = orderId,
            ProductId = productId.ToString(),
            WarehouseId = warehouseId,
            Quantity = quantity
        };

        var reservationTags = new[]
        {
            $"order:{orderId}",
            $"product:{productId}",
            $"warehouse:{warehouseId}",
            "workflow:order-fulfillment",
            "step:inventory-reserved"
        };

        var reservationCondition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags($"order:{orderId}")),
            after: workflowLastPos);

        await _dcbRepository.AppendWithContextAsync(
            new IEvent[] { reservationEvent },
            reservationTags,
            reservationCondition);

        Console.WriteLine($"[ok] Inventory Reserved:");
        Console.WriteLine($"   Warehouse: {warehouseId}");
        Console.WriteLine($"   Quantity: {quantity}");
        Console.WriteLine($"   Coordination: After position {workflowLastPos}");
        Console.WriteLine($"     Coordinates order + warehouse inventory\n");

        // Step 3: Process Payment
        workflowLastPos = await _dcbRepository.GetLastSequencePositionAsync(
            new[] { $"order:{orderId}" });

        var paymentId = Guid.NewGuid();
        var paymentEvent = new PaymentProcessedEvent
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            PaymentId = paymentId,
            OrderId = orderId,
            CustomerId = customerId,
            Amount = 1999.98m // 2 laptops @ $999.99
        };

        var paymentTags = new[]
        {
            $"order:{orderId}",
            $"customer:{customerId}",
            $"payment:{paymentId}",
            "workflow:order-fulfillment",
            "step:payment-processed"
        };

        await _dcbRepository.AppendWithContextAsync(
            new IEvent[] { paymentEvent },
            paymentTags,
            AppendCondition.FailIfMatches(
                Query.FromItems(QueryItem.ByTags($"order:{orderId}")),
                after: workflowLastPos));

        Console.WriteLine($"[ok] Payment Processed:");
        Console.WriteLine($"   Payment ID: {paymentId}");
        Console.WriteLine($"   Amount: ${1999.98m:F2}");
        Console.WriteLine($"   Coordination: After position {workflowLastPos}\n");

        // Step 4: Ship Order
        workflowLastPos = await _dcbRepository.GetLastSequencePositionAsync(
            new[] { $"order:{orderId}" });

        var shipmentId = Guid.NewGuid();
        var shippedEvent = new OrderShippedEvent
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            ShipmentId = shipmentId,
            OrderId = orderId,
            WarehouseId = warehouseId,
            TrackingNumber = "1Z999AA10123456784"
        };

        var shipmentTags = new[]
        {
            $"order:{orderId}",
            $"warehouse:{warehouseId}",
            $"shipment:{shipmentId}",
            "workflow:order-fulfillment",
            "step:order-shipped"
        };

        await _dcbRepository.AppendWithContextAsync(
            new IEvent[] { shippedEvent },
            shipmentTags,
            AppendCondition.FailIfMatches(
                Query.FromItems(QueryItem.ByTags($"order:{orderId}")),
                after: workflowLastPos));

        Console.WriteLine($"[ok] Order Shipped:");
        Console.WriteLine($"   Shipment ID: {shipmentId}");
        Console.WriteLine($"   Tracking: 1Z999AA10123456784");
        Console.WriteLine($"   Warehouse: {warehouseId}");
        Console.WriteLine($"   Coordination: After position {workflowLastPos}\n");

        Console.WriteLine("Why DCB Pattern for Order Fulfillment?");
        Console.WriteLine("* Workflow spans: Order -> Inventory -> Payment -> Shipment");
        Console.WriteLine("* Each step needs to coordinate with previous steps");
        Console.WriteLine("* Can query: 'Show all events for order:{id}'");
        Console.WriteLine("* Can query: 'Show all orders from warehouse:{id}'");
        Console.WriteLine("* Can query: 'Show all shipments for customer:{id}'");
        Console.WriteLine("* AppendCondition ensures step ordering and consistency");
        Console.WriteLine("* No need for saga state - just query tags!");
    }

    private async Task Part4_QueryComparison()
    {
        Console.WriteLine("\n===============================================================");
        Console.WriteLine("PART 4: Query Flexibility Comparison");
        Console.WriteLine("===============================================================\n");

        var productId = _productIds[0];

        Console.WriteLine("  Traditional Aggregate Queries:\n");
        var loaded = await _aggregateRepository.GetOrCreateAsync<Product>(productId);
        Console.WriteLine($"Query: 'Get Product {productId}'");
        Console.WriteLine($"  -> Read stream: '{loaded.StreamId}'");
        Console.WriteLine($"  -> Rebuild Product aggregate from events");
        Console.WriteLine($"  -> Result: Product state (name, price, stock)");
        Console.WriteLine($"  [ok] Simple, direct, efficient for single aggregate\n");

        Console.WriteLine($"Query: 'Get all Products'");
        Console.WriteLine($"  -> Query stream registry for type 'Product'");
        Console.WriteLine($"  -> Returns list of stream IDs");
        Console.WriteLine($"  -> Load each stream individually");
        Console.WriteLine($"  [ok] Works for simple listings\n");

        Console.WriteLine("  DCB Pattern Queries:\n");

        Console.WriteLine($"Query: 'Get all inventory for product:{productId}'");
        var inventoryEventsRaw = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(Query.FromItems(QueryItem.ByTags($"product:{productId}"))))
            inventoryEventsRaw.Add(evt);
        var inventoryEvents = inventoryEventsRaw;
        var warehouseCount = inventoryEvents
            .SelectMany(e => e.Tags)
            .Where(t => t.StartsWith("warehouse:"))
            .Distinct()
            .Count();
        Console.WriteLine($"  -> Query by tag: 'product:{productId}'");
        Console.WriteLine($"  -> Result: {inventoryEvents.Count()} events across {warehouseCount} warehouses");
        Console.WriteLine($"  [ok] Flexible, can see all product activity\n");

        Console.WriteLine($"Query: 'Get all inventory in warehouse:WH-WEST'");
        var warehouseEventsRaw = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(Query.FromItems(QueryItem.ByTags($"warehouse:WH-WEST"))))
            warehouseEventsRaw.Add(evt);
        var warehouseEvents = warehouseEventsRaw;
        var productCount = warehouseEvents
            .SelectMany(e => e.Tags)
            .Where(t => t.StartsWith("product:"))
            .Distinct()
            .Count();
        Console.WriteLine($"  -> Query by tag: 'warehouse:WH-WEST'");
        Console.WriteLine($"  -> Result: {warehouseEvents.Count()} events for {productCount} product(s)");
        Console.WriteLine($"  [ok] Can't do this easily with traditional aggregates!\n");

        Console.WriteLine($"Query: 'Get all order fulfillment workflows'");
        var workflowEventsRaw = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(Query.FromItems(QueryItem.ByTags($"workflow:order-fulfillment"))))
            workflowEventsRaw.Add(evt);
        var workflowEvents = workflowEventsRaw;
        Console.WriteLine($"  -> Query by tag: 'workflow:order-fulfillment'");
        Console.WriteLine($"  -> Result: {workflowEvents.Count()} events across all workflows");
        Console.WriteLine($"  [ok] Perfect for process monitoring and analytics\n");

        Console.WriteLine("  Key Insight:");
        Console.WriteLine("   Traditional: Great for 'Get THIS entity'");
        Console.WriteLine("   DCB: Great for 'Find all entities WHERE...'");
    }

    private async Task ShowArchitecturalGuidance()
    {
        Console.WriteLine("\n===============================================================");
        Console.WriteLine("ARCHITECTURAL GUIDANCE: Hybrid Approach");
        Console.WriteLine("===============================================================\n");

        Console.WriteLine("[ok] RECOMMENDED ARCHITECTURE:\n");

        Console.WriteLine("1   TRADITIONAL AGGREGATES for Core Entities:");
        Console.WriteLine("   * User accounts and profiles");
        Console.WriteLine("   * Product catalog (as shown)");
        Console.WriteLine("   * Customer master data");
        Console.WriteLine("   * Configuration entities");
        Console.WriteLine("   -> Use: IAggregateRepository + AggregateRoot<TState>\n");

        Console.WriteLine("2   DCB PATTERN for Complex Workflows:");
        Console.WriteLine("   * Order fulfillment (as shown)");
        Console.WriteLine("   * Inventory management (as shown)");
        Console.WriteLine("   * Payment processing");
        Console.WriteLine("   * Shipping and logistics");
        Console.WriteLine("   * Cross-entity coordination");
        Console.WriteLine("   -> Use: IDcbRepository + DcbEntity<TState> or AppendWithContextAsync\n");

        Console.WriteLine("3   SERVICE REGISTRATION (both together):");
        Console.WriteLine("   services.AddLawnDart(o => o.RequireTenantId = false);");
        Console.WriteLine("   services.AddBoundedContext(\"default\")");
        Console.WriteLine("       .UseInMemory()");
        Console.WriteLine("       .WithEventTypes<ProductCreated>();");
        Console.WriteLine("   The context registers IAggregateRepository and IDcbRepository.");
        Console.WriteLine();

        Console.WriteLine("4   INTEGRATION PATTERNS:");
        Console.WriteLine("   * Aggregate can emit events with tags (becomes DCB-queryable)");
        Console.WriteLine("   * DCB reactor can load aggregate and execute command");
        Console.WriteLine("   * Both share same EventStore, Metadata, Authorization");
        Console.WriteLine("   * Use projections to build read models from both\n");

        Console.WriteLine("[warn]  ANTI-PATTERNS TO AVOID:");
        Console.WriteLine("   [x] Using DCB for everything (adds unnecessary complexity)");
        Console.WriteLine("   [x] Using Aggregates for cross-entity workflows (brittle)");
        Console.WriteLine("   [x] Mixing patterns within same entity (confusing)");
        Console.WriteLine("   [x] Creating aggregates that are just DCB wrappers\n");

        Console.WriteLine("  DECISION CHECKLIST:");
        Console.WriteLine("   Ask: 'Does this entity have clear, stable boundaries?'");
        Console.WriteLine("     -> YES: Use Traditional Aggregate");
        Console.WriteLine("     -> NO: Consider DCB");
        Console.WriteLine("   ");
        Console.WriteLine("   Ask: 'Do I need to query this by multiple dimensions?'");
        Console.WriteLine("     -> YES: Use DCB");
        Console.WriteLine("     -> NO: Traditional Aggregate is fine");
        Console.WriteLine("   ");
        Console.WriteLine("   Ask: 'Does this involve cross-entity coordination?'");
        Console.WriteLine("     -> YES: Use DCB");
        Console.WriteLine("     -> NO: Traditional Aggregate is simpler");

        Console.WriteLine("\n===============================================================");
        Console.WriteLine("Demo Complete! Both patterns working in harmony.  ");
        Console.WriteLine("===============================================================\n");

        await Task.CompletedTask;
    }
}

// DCB-specific event types for the demo
[EventTypeName("inventory-allocated-event")]
public record InventoryAllocatedEvent : IEvent
{
    public Guid Id { get; init; }
    public DateTime Timestamp { get; init; }
    public string ProductId { get; init; } = string.Empty;
    public string WarehouseId { get; init; } = string.Empty;
    public int Quantity { get; init; }
}

[EventTypeName("inventory-transferred-event")]
public record InventoryTransferredEvent : IEvent
{
    public Guid Id { get; init; }
    public DateTime Timestamp { get; init; }
    public Guid TransferId { get; init; }
    public string ProductId { get; init; } = string.Empty;
    public string FromWarehouse { get; init; } = string.Empty;
    public string ToWarehouse { get; init; } = string.Empty;
    public int Quantity { get; init; }
    public string Direction { get; init; } = string.Empty; // "OUT" or "IN"
}

[EventTypeName("order-created-event")]
public record OrderCreatedEvent : IEvent
{
    public Guid Id { get; init; }
    public DateTime Timestamp { get; init; }
    public Guid OrderId { get; init; }
    public string CustomerId { get; init; } = string.Empty;
    public string ProductId { get; init; } = string.Empty;
    public int Quantity { get; init; }
}

[EventTypeName("inventory-reserved-event")]
public record InventoryReservedEvent : IEvent
{
    public Guid Id { get; init; }
    public DateTime Timestamp { get; init; }
    public Guid OrderId { get; init; }
    public string ProductId { get; init; } = string.Empty;
    public string WarehouseId { get; init; } = string.Empty;
    public int Quantity { get; init; }
}

[EventTypeName("payment-processed-event")]
public record PaymentProcessedEvent : IEvent
{
    public Guid Id { get; init; }
    public DateTime Timestamp { get; init; }
    public Guid PaymentId { get; init; }
    public Guid OrderId { get; init; }
    public string CustomerId { get; init; } = string.Empty;
    public decimal Amount { get; init; }
}

[EventTypeName("hybrid-order-shipped-event")]
public record OrderShippedEvent : IEvent
{
    public Guid Id { get; init; }
    public DateTime Timestamp { get; init; }
    public Guid ShipmentId { get; init; }
    public Guid OrderId { get; init; }
    public string WarehouseId { get; init; } = string.Empty;
    public string TrackingNumber { get; init; } = string.Empty;
}
