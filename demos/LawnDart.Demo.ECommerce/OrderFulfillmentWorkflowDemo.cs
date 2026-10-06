using Microsoft.Extensions.Logging;
using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Demo.ECommerce;

/// <summary>
/// Demonstrates cross-aggregate workflow using DCB pattern.
/// Four separate appends. Each append after the first fails if the workflow
/// tag moved. A crash between steps leaves the earlier steps in place.
/// </summary>
public class OrderFulfillmentWorkflowDemo
{
    private readonly IDcbRepository _dcbRepository;
    private readonly IMetadataProvider _metadataProvider;
    private readonly ILogger<OrderFulfillmentWorkflowDemo> _logger;

    public OrderFulfillmentWorkflowDemo(
        IDcbRepository dcbRepository,
        IMetadataProvider metadataProvider,
        ILogger<OrderFulfillmentWorkflowDemo> logger)
    {
        _dcbRepository = dcbRepository;
        _metadataProvider = metadataProvider;
        _logger = logger;
    }

    public async Task RunAsync()
    {
        Console.WriteLine("\n+===============================================================+");
        Console.WriteLine("|        DCB CROSS-AGGREGATE WORKFLOW DEMONSTRATION              |");
        Console.WriteLine("+===============================================================+\n");

        Console.WriteLine("This demo shows how DCB enables workflows across multiple aggregates:");
        Console.WriteLine("  * Order -> Inventory -> Payment -> Shipment");
        Console.WriteLine("  * Each step tagged with workflow:{{id}} for coordination");
        Console.WriteLine("  * Each later append fences the workflow tag.");
        Console.WriteLine("  * The four steps are four appends. A crash between them leaves the earlier steps.\n");

        var workflowId = Guid.NewGuid();
        var orderId = Guid.NewGuid();
        var customerId = "customer-456";
        var productId = "product-123";

        await Step1_CreateOrder(workflowId, orderId, customerId, productId);
        await Step2_ReserveInventory(workflowId, orderId, productId);
        await Step3_ProcessPayment(workflowId, orderId, customerId);
        await Step4_ShipOrder(workflowId, orderId);
        await Step5_QueryWorkflow(workflowId);
    }

    private async Task Step1_CreateOrder(Guid workflowId, Guid orderId, string customerId, string productId)
    {
        Console.WriteLine("=== Step 1: Create Order ===");
        
        var orderEvent = new WorkflowEvent
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            Type = "OrderCreated",
            EntityId = orderId.ToString(),
            Data = $"Customer: {customerId}, Product: {productId}, Qty: 1"
        };

        var tags = new[]
        {
            $"workflow:{workflowId}",
            $"order:{orderId}",
            $"customer:{customerId}",
            "step:order-created"
        };

        // No condition for first step
        await _dcbRepository.AppendWithContextAsync(
            new IEvent[] { orderEvent },
            tags);

        Console.WriteLine($"  [ok] Order created: {orderId}");
        Console.WriteLine($"    Tags: {string.Join(", ", tags)}");
        Console.WriteLine($"    Workflow: {workflowId}");
        Console.WriteLine();
    }

    private async Task Step2_ReserveInventory(Guid workflowId, Guid orderId, string productId)
    {
        Console.WriteLine("=== Step 2: Reserve Inventory ===");
        
        // Get last position for workflow to ensure ordering
        var workflowTag = $"workflow:{workflowId}";
        var lastPosition = await _dcbRepository.GetLastSequencePositionAsync(new[] { workflowTag });

        var inventoryEvent = new WorkflowEvent
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            Type = "InventoryReserved",
            EntityId = productId,
            Data = $"Reserved 1 unit for order {orderId}"
        };

        var tags = new[]
        {
            $"workflow:{workflowId}",
            $"order:{orderId}",
            $"product:{productId}",
            "step:inventory-reserved"
        };

        // Condition: Ensure no workflow events have been added since we last checked
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags(workflowTag)),
            after: lastPosition);

        await _dcbRepository.AppendWithContextAsync(
            new IEvent[] { inventoryEvent },
            tags,
            condition);

        Console.WriteLine($"  [ok] Inventory reserved for product: {productId}");
        Console.WriteLine($"    Tags: {string.Join(", ", tags)}");
        Console.WriteLine($"    Condition: After position {lastPosition}");
        Console.WriteLine();
    }

    private async Task Step3_ProcessPayment(Guid workflowId, Guid orderId, string customerId)
    {
        Console.WriteLine("=== Step 3: Process Payment ===");
        
        var workflowTag = $"workflow:{workflowId}";
        var lastPosition = await _dcbRepository.GetLastSequencePositionAsync(new[] { workflowTag });

        var paymentId = Guid.NewGuid();
        var paymentEvent = new WorkflowEvent
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            Type = "PaymentProcessed",
            EntityId = paymentId.ToString(),
            Data = $"Charged customer {customerId} for order {orderId}, Amount: $29.99"
        };

        var tags = new[]
        {
            $"workflow:{workflowId}",
            $"order:{orderId}",
            $"payment:{paymentId}",
            $"customer:{customerId}",
            "step:payment-processed"
        };

        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags(workflowTag)),
            after: lastPosition);

        await _dcbRepository.AppendWithContextAsync(
            new IEvent[] { paymentEvent },
            tags,
            condition);

        Console.WriteLine($"  [ok] Payment processed: {paymentId}");
        Console.WriteLine($"    Amount: $29.99");
        Console.WriteLine($"    Tags: {string.Join(", ", tags)}");
        Console.WriteLine($"    Condition: After position {lastPosition}");
        Console.WriteLine();
    }

    private async Task Step4_ShipOrder(Guid workflowId, Guid orderId)
    {
        Console.WriteLine("=== Step 4: Ship Order ===");
        
        var workflowTag = $"workflow:{workflowId}";
        var lastPosition = await _dcbRepository.GetLastSequencePositionAsync(new[] { workflowTag });

        var shipmentId = Guid.NewGuid();
        var shipmentEvent = new WorkflowEvent
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            Type = "OrderShipped",
            EntityId = shipmentId.ToString(),
            Data = $"Shipped order {orderId}, Tracking: 1Z999AA10123456784"
        };

        var tags = new[]
        {
            $"workflow:{workflowId}",
            $"order:{orderId}",
            $"shipment:{shipmentId}",
            "step:order-shipped"
        };

        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags(workflowTag)),
            after: lastPosition);

        await _dcbRepository.AppendWithContextAsync(
            new IEvent[] { shipmentEvent },
            tags,
            condition);

        Console.WriteLine($"  [ok] Order shipped: {shipmentId}");
        Console.WriteLine($"    Tracking: 1Z999AA10123456784");
        Console.WriteLine($"    Tags: {string.Join(", ", tags)}");
        Console.WriteLine($"    Condition: After position {lastPosition}");
        Console.WriteLine();
    }

    private async Task Step5_QueryWorkflow(Guid workflowId)
    {
        Console.WriteLine("=== Step 5: Query Entire Workflow ===");
        
        // Query all events for this workflow
        var workflowTag = $"workflow:{workflowId}";
        var state = await _dcbRepository.GetStateAsync<WorkflowState>(new[] { workflowTag });

        Console.WriteLine($"\nWorkflow {workflowId} Summary:");
        Console.WriteLine($"  Total Events: {state.EventCount}");
        if (state.EventCount != 4)
            throw new InvalidOperationException($"Expected 4 workflow events, got {state.EventCount}.");
        Console.WriteLine($"  Steps Completed:");
        
        foreach (var step in state.Steps)
        {
            Console.WriteLine($"    {step.Sequence}. {step.Type} ({step.Timestamp:HH:mm:ss})");
            Console.WriteLine($"       Entity: {step.EntityId}");
            Console.WriteLine($"       {step.Data}");
        }

        Console.WriteLine("\n=== DCB Benefits Demonstrated ===");
        Console.WriteLine("  [ok] No aggregate roots - just events with tags");
        Console.WriteLine("  [ok] Cross-entity consistency via AppendCondition");
        Console.WriteLine("  [ok] Flexible querying by any tag combination");
        Console.WriteLine("  [ok] Workflow coordination without saga state");
        Console.WriteLine("  [ok] Easy to query: 'Show all events for workflow:{id}'");
        Console.WriteLine();
    }
}

/// <summary>
/// Simple workflow event for demonstration.
/// </summary>
[EventTypeName("workflow-event")]
public record WorkflowEvent : IEvent
{
    public Guid Id { get; init; }
    public DateTime Timestamp { get; init; }
    public string Type { get; init; } = string.Empty;
    public string EntityId { get; init; } = string.Empty;
    public string Data { get; init; } = string.Empty;
}

/// <summary>
/// Workflow state built from all tagged events.
/// </summary>
public class WorkflowState : IState
{
    public int EventCount { get; private set; }
    public List<WorkflowStep> Steps { get; } = new();

    public void Apply(WorkflowEvent @event)
    {
        EventCount++;
        Steps.Add(new WorkflowStep
        {
            Sequence = EventCount,
            Type = @event.Type,
            EntityId = @event.EntityId,
            Data = @event.Data,
            Timestamp = @event.Timestamp
        });
    }
}

public class WorkflowStep
{
    public int Sequence { get; init; }
    public string Type { get; init; } = string.Empty;
    public string EntityId { get; init; } = string.Empty;
    public string Data { get; init; } = string.Empty;
    public DateTime Timestamp { get; init; }
}
