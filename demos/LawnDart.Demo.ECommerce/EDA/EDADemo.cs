using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;

namespace LawnDart.Demo.ECommerce.EDA;

/// <summary>
/// Demonstrates the three EDA reactivity patterns using an in-memory transport:
/// <list type="bullet">
///   <item><b>Reactor</b>: <see cref="OrderShipped"/> -> <see cref="SendNotificationCommand"/></item>
///   <item><b>Event Processor</b>: <see cref="OrderPlaced"/> -> <see cref="InventoryReserved"/> (derived event)</item>
///   <item><b>Task Processor</b>: polled state -> <see cref="CancelOverdueOrderCommand"/></item>
/// </list>
/// No external broker required - <see cref="InMemoryMessageTransport"/> delivers messages in-process.
/// </summary>
public class EDADemo
{
    public async Task RunAsync()
    {
        Console.WriteLine();
        Console.WriteLine("=== EDA ===");
        Console.WriteLine("Transport: in-process. No broker.");
        Console.WriteLine("Inbox: this walk keys by message id for the subscriber in that section.");
        Console.WriteLine("A hosted reactor keys by consumer type plus message id.");
        Console.WriteLine("SQL Server inbox dedup is AddSqlInboxStore. This walk stays on the in-memory inbox.");
        Console.WriteLine();

        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var inbox = new InMemoryInboxStore(Options.Create(new MessagingOptions()));

        // -- Pattern 1: Reactor ------------------------------------------------
        await DemonstrateReactor(transport, inbox);

        // -- Pattern 2: Event Processor ---------------------------------------
        await DemonstrateEventProcessor(transport, inbox);

        // -- Pattern 3: Task Processor ----------------------------------------
        await DemonstrateTaskProcessor();

        // -- Pattern 4: Full Workflow (Choreography + DCB) ---------------------
        await DemonstrateFullWorkflow();

        Console.WriteLine("\n=== EDA Demo Complete ===");
    }

    // -------------------------------------------------------------------------
    // Pattern 1 - Reactor: Event (broker) -> Command
    // -------------------------------------------------------------------------
    private static async Task DemonstrateReactor(
        InMemoryMessageTransport transport,
        InMemoryInboxStore inbox)
    {
        Console.WriteLine("--- Pattern 1: Reactor (Event -> Command) -------------------------------");
        Console.WriteLine("Scenario: When an order ships, notify the customer.\n");

        var reactor = new OrderReactor();
        var dispatchedCommands = new List<ICommand>();

        // Subscribe: OrderShipped -> reactor -> SendNotificationCommand
        _ = transport.SubscribeAsync<OrderShipped>(async (e, ctx, ct) =>
        {
            if (ctx.MessageId is not null && await inbox.IsProcessedAsync(ctx.MessageId, ct))
            {
                Console.WriteLine($"    [inbox] Duplicate MessageId={ctx.MessageId} - skipped.");
                return;
            }

            var commands = (await reactor.ReactAsync(e, ctx, ct)).ToList();

            if (ctx.MessageId is not null)
                await inbox.MarkProcessedAsync(ctx.MessageId, DateTimeOffset.UtcNow, ct);

            foreach (var cmd in commands)
            {
                dispatchedCommands.Add(cmd);
                var notif = (SendNotificationCommand)cmd;
                Console.WriteLine($"  [ok] Command dispatched -> {nameof(SendNotificationCommand)}");
                Console.WriteLine($"      OrderId:    {notif.OrderId}");
                Console.WriteLine($"      CustomerId: {notif.CustomerId}");
                Console.WriteLine($"      Message:    {notif.Message}");
            }
        });

        // Publish: first delivery
        var event1 = new OrderShipped(
            Id: Guid.NewGuid(),
            Timestamp: DateTime.UtcNow,
            OrderId: "order-1001",
            CustomerId: "cust-42",
            TrackingNumber: "TRK-555-ABC");

        var ctx1 = new MessageContext { MessageId = "msg-ship-001", CorrelationId = "corr-001" };
        Console.WriteLine($"Publishing OrderShipped (MessageId={ctx1.MessageId})...");
        await transport.PublishAsync(event1, ctx1);

        Console.WriteLine();
        Console.WriteLine("  -- Idempotency test: same MessageId delivered a second time --");
        Console.WriteLine($"Publishing OrderShipped again (MessageId={ctx1.MessageId})...");
        await transport.PublishAsync(event1, ctx1);

        Console.WriteLine($"\n  Reactor invocations: {dispatchedCommands.Count} command(s) dispatched " +
                          "(1 expected. The second delivery used the same message id).\n");
        if (dispatchedCommands.Count != 1)
            throw new InvalidOperationException($"Expected 1 notification command, got {dispatchedCommands.Count}.");
    }

    // -------------------------------------------------------------------------
    // Pattern 2 - Event Processor: Event (broker) -> Derived Event
    // -------------------------------------------------------------------------
    private static async Task DemonstrateEventProcessor(
        InMemoryMessageTransport transport,
        InMemoryInboxStore inbox)
    {
        Console.WriteLine("--- Pattern 2: Event Processor (Event -> Derived Event) -----------------");
        Console.WriteLine("Scenario: When an order is placed, reserve inventory.\n");

        var processor = new InventoryEventProcessor();
        var derivedEvents = new List<InventoryReserved>();

        // Subscribe to derived InventoryReserved events so we can observe them
        _ = transport.SubscribeAsync<InventoryReserved>((e, ctx, _) =>
        {
            derivedEvents.Add(e);
            Console.WriteLine($"  [ok] Derived event received -> {nameof(InventoryReserved)}");
            Console.WriteLine($"      OrderId:          {e.OrderId}");
            Console.WriteLine($"      QuantityReserved: {e.QuantityReserved}");
            Console.WriteLine($"      CausationId:      {ctx.CausationId} (  parent MessageId)");
            return Task.CompletedTask;
        });

        // Subscribe: OrderPlaced -> processor -> InventoryReserved (re-published)
        _ = transport.SubscribeAsync<OrderPlaced>(async (e, ctx, ct) =>
        {
            if (ctx.MessageId is not null && await inbox.IsProcessedAsync(ctx.MessageId, ct))
            {
                Console.WriteLine($"    [inbox] Duplicate MessageId={ctx.MessageId} - skipped.");
                return;
            }

            var events = await processor.ProcessAsync(e, ctx, ct);

            if (ctx.MessageId is not null)
                await inbox.MarkProcessedAsync(ctx.MessageId, DateTimeOffset.UtcNow, ct);

            // Publish derived events back through transport using runtime-type dispatch
            foreach (var derived in events)
                await transport.PublishEventAsync(derived, ctx.CreateChild(), ct);
        });

        // Publish: first delivery
        var event1 = new OrderPlaced(
            Id: Guid.NewGuid(),
            Timestamp: DateTime.UtcNow,
            OrderId: "order-2001",
            CustomerId: "cust-99",
            Amount: 149.95m);

        var ctx1 = new MessageContext { MessageId = "msg-placed-001", CorrelationId = "corr-002" };
        Console.WriteLine($"Publishing OrderPlaced (MessageId={ctx1.MessageId})...");
        await transport.PublishAsync(event1, ctx1);

        // Publish a second, different order
        var event2 = new OrderPlaced(
            Id: Guid.NewGuid(),
            Timestamp: DateTime.UtcNow,
            OrderId: "order-2002",
            CustomerId: "cust-77",
            Amount: 299.00m);

        var ctx2 = new MessageContext { MessageId = "msg-placed-002", CorrelationId = "corr-003" };
        Console.WriteLine($"\nPublishing second OrderPlaced (MessageId={ctx2.MessageId})...");
        await transport.PublishAsync(event2, ctx2);

        Console.WriteLine();
        Console.WriteLine($"  Derived events received: {derivedEvents.Count} (2 expected, one per order).");
        Console.WriteLine();
        if (derivedEvents.Count != 2)
            throw new InvalidOperationException($"Expected 2 inventory events, got {derivedEvents.Count}.");
    }

    // -------------------------------------------------------------------------
    // Pattern 3 - Task Processor: Polled State -> Commands
    // -------------------------------------------------------------------------
    private static async Task DemonstrateTaskProcessor()
    {
        Console.WriteLine("--- Pattern 3: Task Processor (Polled State -> Commands) ----------------");
        Console.WriteLine("Scenario: Periodically scan for overdue orders and cancel them.\n");

        var overdueOrders = new[] { "order-0101", "order-0202", "order-0303" };
        var taskProcessor = new OverdueOrderTaskProcessor(overdueOrders);

        Console.WriteLine($"Polling task processor ({overdueOrders.Length} simulated overdue orders)...");

        var commands = (await taskProcessor.ProcessTasksAsync()).ToList();

        foreach (var cmd in commands.Cast<CancelOverdueOrderCommand>())
        {
            Console.WriteLine($"  [ok] Command dispatched -> {nameof(CancelOverdueOrderCommand)}");
            Console.WriteLine($"      OrderId: {cmd.OrderId}");
            Console.WriteLine($"      Reason:  {cmd.Reason}");
        }

        Console.WriteLine();
        Console.WriteLine($"  Commands emitted: {commands.Count} (one per overdue order).");
        Console.WriteLine();
        if (commands.Count != overdueOrders.Length)
            throw new InvalidOperationException($"Expected {overdueOrders.Length} cancel commands, got {commands.Count}.");
    }

    // -------------------------------------------------------------------------
    // Pattern 4 - Full Workflow: multi-step saga via EDA choreography + DCB
    // -------------------------------------------------------------------------
    private static async Task DemonstrateFullWorkflow()
    {
        Console.WriteLine("--- Pattern 4: Full Workflow - End-to-End Order Fulfilment Saga --------");
        Console.WriteLine("""
            This pattern composes Reactor + Event Processor + Command Dispatcher
            into a complete multi-step business workflow.
            Two implementations are shown side-by-side:
              Part A - EDA Choreography  (broker-driven, at-least-once + inbox)
              Part B - DCB In-Process    (one pass over an in-memory log, no broker)
            """);

        await OrderFulfillmentWorkflow.RunChoreographyAsync();
        await OrderFulfillmentWorkflow.RunDcbAsync();
    }
}
