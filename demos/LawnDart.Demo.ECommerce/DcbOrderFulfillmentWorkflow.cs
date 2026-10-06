using LawnDart.EventStore;
using LawnDart.Metadata;
using ConcurrencyException = LawnDart.EventStore.ConcurrencyException;

namespace LawnDart.Demo.ECommerce;

/// <summary>
/// Demonstrates DCB (Dynamic Consistency Boundaries) pattern for order fulfillment workflow.
/// 
/// DCB enables flexible consistency enforcement without rigid transactional boundaries.
/// One append writes the order, payment, and inventory events together.
/// A later step is a separate append. The append condition fences the tags
/// this decision read.
/// 
/// Key Concepts:
/// - Query-Based Consistency: The condition matches tags, not one aggregate stream
/// - Sequence Position Concurrency: Optimistic concurrency using global sequence positions
/// - One append: all three events are written together, or none are
/// - Cross-Entity Queries: Query those writes by tag
/// </summary>
public class DcbOrderFulfillmentWorkflow
{
    private readonly IEventStore _eventStore;
    private readonly IMetadataProvider _metadataProvider;

    public DcbOrderFulfillmentWorkflow(IEventStore eventStore, IMetadataProvider metadataProvider)
    {
        _eventStore = eventStore;
        _metadataProvider = metadataProvider;
    }

    /// <summary>
    /// Executes an order fulfillment workflow using DCB pattern.
    /// 
    /// One <c>AppendAsync</c> writes the three events. The condition fails
    /// when a matching tag moved after the read. A second call is a new append.
    /// </summary>
    public async Task ExecuteWorkflowAsync(
        Guid orderId, 
        Guid paymentId, 
        string productId, 
        int quantity)
    {
        Console.WriteLine("\n=== DCB Order Fulfillment Workflow ===");
        Console.WriteLine($"Order: {orderId}, Payment: {paymentId}, Product: {productId}, Quantity: {quantity}");

        // Step 1: Read current state to get last known sequence position
        // In DCB, we use tags to identify related entities across different streams
        // Tags follow the pattern: "{entityType}:{entityId}"
        var orderTag = $"order:{orderId}";
        var paymentTag = $"payment:{paymentId}";
        var productTag = $"product:{productId}";

        // Create a query that matches events for any of our related entities
        // This query uses OR logic - it matches if ANY query item matches
        var query = Query.FromItems(
            QueryItem.ByTags(orderTag),
            QueryItem.ByTags(paymentTag),
            QueryItem.ByTags(productTag)
        );

        // Read existing events to determine the last known sequence position
        // This position represents the highest sequence we were aware of when building our decision
        var existingEvents = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(query))
            existingEvents.Add(evt);
        var lastPosition = existingEvents.Count > 0 
            ? existingEvents.Max(e => e.SequencePosition) 
            : 0L;

        Console.WriteLine($"Last known sequence position: {lastPosition}");
        Console.WriteLine($"Existing events found: {existingEvents.Count}");

        // Step 2: Create append condition to ensure no conflicting events were added
        // The condition says: "Fail if any events matching our query exist AFTER lastPosition"
        // This enforces consistency: if any events matching our query exist after lastPosition,
        // it means another process modified the order/payment/inventory state, and we should fail
        var condition = AppendCondition.FailIfMatches(query, after: lastPosition);

        // Step 3: Prepare workflow events (spanning multiple entities)
        // In a real scenario, these would be domain events from different aggregates
        // Here we're simulating a workflow that affects Order, Payment, and Inventory
        var workflowEvents = new IEvent[]
        {
            new OrderFulfillmentStarted(Guid.NewGuid(), DateTime.UtcNow, orderId, paymentId, productId),
            new PaymentAuthorized(Guid.NewGuid(), DateTime.UtcNow, paymentId, orderId),
            new InventoryReserved(Guid.NewGuid(), DateTime.UtcNow, productId, quantity, orderId)
        };

        // One append writes all three events. The store assigns the stream id
        // (dcb:{guid}, or {tenant}:dcb:{guid} when metadata carries a tenant).
        // The workflow tag is for queries. It is not the stream id.
        var workflowId = Guid.NewGuid();
        var workflowTag = $"workflow:{workflowId}";
        
        // All events are tagged with workflow ID AND entity IDs for cross-entity querying
        // This allows us to query all events related to an order, even if they're in different streams
        var commandMetadata = _metadataProvider.CaptureCommandMetadata();
        var tenantId = commandMetadata.TenantId;
        commandMetadata.UserId = "fulfillment-service";
        commandMetadata.CorrelationId = Guid.NewGuid().ToString();

        // Convert CommandMetadata to EventMetadata for event store
        var eventMetadata = new EventMetadata
        {
            UserId = commandMetadata.UserId,
            TenantId = tenantId,
            CorrelationId = commandMetadata.CorrelationId,
            EventId = Guid.NewGuid().ToString(),
            Timestamp = DateTime.UtcNow
        };
        
        try
        {
            // One append: all three events, or none.
            var appendResult = await _eventStore.AppendAsync(
                workflowEvents,
                condition,
                eventMetadata,
                tags: new[] { workflowTag, orderTag, paymentTag, productTag });

            Console.WriteLine();
            Console.WriteLine("[ok] Workflow events appended in one write.");
            Console.WriteLine($"Workflow ID: {workflowId}");
            Console.WriteLine($"Sequence positions: {string.Join(", ", appendResult.SequencePositions)}");
            Console.WriteLine($"Events tagged with: {workflowTag}, {orderTag}, {paymentTag}, {productTag}");

            // Step 5: Verify cross-entity query
            // Query all events related to the order, regardless of which stream they're in
            // This demonstrates DCB's ability to query across entity boundaries
            var verificationQuery = Query.FromItems(QueryItem.ByTags(orderTag));
            var allOrderEvents = (await _eventStore.ReadByQueryAsync(verificationQuery)).Events;
            
            Console.WriteLine();
            Console.WriteLine("Cross-entity query results:");
            Console.WriteLine($"Total events for order {orderId}: {allOrderEvents.Count}");
            foreach (var evt in allOrderEvents.OrderBy(e => e.SequencePosition))
            {
                Console.WriteLine($"  - Position {evt.SequencePosition}: {evt.Event.GetType().Name} stream {evt.StreamId}");
            }

            if (appendResult.SequencePositions.Count != 3 || allOrderEvents.Count < 3)
            {
                throw new InvalidOperationException(
                    $"Expected one append of 3 events for order {orderId}, got positions {appendResult.SequencePositions.Count} and query count {allOrderEvents.Count}.");
            }
        }
        catch (ConcurrencyException ex)
        {
            Console.WriteLine($"\n[x] Workflow failed due to concurrency conflict!");
            Console.WriteLine($"Expected position: {ex.ExpectedPosition}, Actual: {ex.ActualPosition}");
            Console.WriteLine($"This means another process modified the order/payment/inventory state.");
            Console.WriteLine($"The application should retry the workflow with updated state.");
            throw;
        }
    }

    /// <summary>
    /// Demonstrates concurrent workflow conflict detection using DCB.
    /// 
    /// This shows how DCB prevents duplicate workflows from running simultaneously.
    /// When two workflows try to start fulfillment for the same order, the second one
    /// will detect the conflict and fail, preventing duplicate processing.
    /// </summary>
    public async Task DemonstrateConcurrencyConflictAsync(Guid orderId, Guid paymentId, string productId)
    {
        Console.WriteLine("\n=== DCB Concurrency Conflict Demonstration ===");
        Console.WriteLine("This demonstrates how DCB prevents duplicate workflows from running concurrently.");

        var orderTag = $"order:{orderId}";
        
        // Each append gets its own store stream. The condition matches the order tag.
        var workflow1Id = Guid.NewGuid();
        var workflow1Tag = $"workflow:{workflow1Id}";
        var workflow2Id = Guid.NewGuid();
        var workflow2Tag = $"workflow:{workflow2Id}";

        // First workflow starts - reads state and finds no existing events
        var query1 = Query.FromItems(QueryItem.ByTags(orderTag));
        var condition1 = AppendCondition.FailIfMatches(query1, after: 0);
        
        var events1 = new IEvent[] 
        { 
            new OrderFulfillmentStarted(Guid.NewGuid(), DateTime.UtcNow, orderId, paymentId, productId) 
        };

        Console.WriteLine("\nWorkflow 1: Appending fulfillment started event...");
        // Use workflow tag first to avoid streamId conflicts
        var appendResult1 = await _eventStore.AppendAsync(events1, condition1, tags: new[] { workflow1Tag, orderTag });
        Console.WriteLine($"[ok] Workflow 1 succeeded at position {appendResult1.SequencePositions[0]}");

        // Second workflow tries to start (should fail)
        // It reads state from position 0, but Workflow 1 already appended an event
        // The condition will detect the conflict and fail (even though they use different streamIds)
        // because the query matches events by tags, not by streamId
        var query2 = Query.FromItems(QueryItem.ByTags(orderTag));
        var condition2 = AppendCondition.FailIfMatches(query2, after: 0); // Still checking from position 0
        
        var events2 = new IEvent[] 
        { 
            new OrderFulfillmentStarted(Guid.NewGuid(), DateTime.UtcNow, orderId, paymentId, productId) 
        };

        Console.WriteLine();
        Console.WriteLine("Workflow 2: Attempting to start fulfillment (should fail)...");
        Console.WriteLine("Workflow 2 still fences from position 0. Workflow 1 already appended an event.");
        Console.WriteLine("The condition matches the order tag, so the second append fails.");
        try
        {
            await _eventStore.AppendAsync(events2, condition2, tags: new[] { workflow2Tag, orderTag });
            throw new InvalidOperationException("Workflow 2 should have failed the append condition.");
        }
        catch (ConcurrencyException)
        {
            Console.WriteLine("[ok] Workflow 2 detected the conflict and failed.");
            Console.WriteLine("A retry would read the new position and decide again.");
        }
    }

    // Event types for the workflow
    // In a real application, these would be domain events from the respective aggregates
    [EventTypeName("order-fulfillment-started")]
    public record OrderFulfillmentStarted(
        Guid Id, 
        DateTime Timestamp, 
        Guid OrderId, 
        Guid PaymentId, 
        string ProductId) : IEvent;

    [EventTypeName("workflow-payment-authorized")]
    public record PaymentAuthorized(
        Guid Id, 
        DateTime Timestamp, 
        Guid PaymentId, 
        Guid OrderId) : IEvent;

    [EventTypeName("workflow-inventory-reserved")]
    public record InventoryReserved(
        Guid Id, 
        DateTime Timestamp, 
        string ProductId, 
        int Quantity, 
        Guid OrderId) : IEvent;
}

