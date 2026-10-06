using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart.Dcb;
using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;
using LawnDart.Metadata;
using LawnDart.Patterns.EventProcessing;
using LawnDart.Patterns.Reaction;

namespace LawnDart.Demo.ECommerce.EDA;

/// <summary>
/// Demonstrates the complete order-fulfilment saga using two coordination approaches:
/// <list type="bullet">
///   <item>
///     <b>Part A - EDA Choreography:</b> broker-driven chain using
///     <see cref="IReactor{TEvent}"/>, <see cref="IEventProcessor{TEvent}"/>, and a
///     <see cref="WorkflowCommandDispatcher"/> that simulates in-memory aggregate handling.
///   </item>
///   <item>
///     <b>Part B - DCB In-Process:</b> same saga without a broker, using
///     <see cref="DcbReactor{TEvent}"/> implementations that poll an in-memory event log.
///   </item>
/// </list>
/// </summary>
public class OrderFulfillmentWorkflow
{
    // -------------------------------------------------------------------------
    // Part A - EDA Choreography via InMemoryMessageTransport
    //
    // Chain:
    //   OrderPlaced
    //     -> [InventoryEventProcessor] -> InventoryReserved (derived event)
    //     -> [PaymentReactor] -> ProcessPaymentCommand -> [WorkflowCommandDispatcher]
    //                       -> PaymentAuthorized (simulated aggregate)
    //     -> [FulfillmentReactor] -> ShipOrderCommand -> [WorkflowCommandDispatcher]
    //                           -> OrderShipped (simulated aggregate)
    //     -> [OrderReactor] -> SendNotificationCommand
    // -------------------------------------------------------------------------

    public static async Task RunChoreographyAsync()
    {
        Console.WriteLine("--- Part A: EDA Choreography (broker-driven multi-step saga) -----------");
        Console.WriteLine("""
            Chain: OrderPlaced
                   -> [InventoryEventProcessor] -> InventoryReserved
                   -> [PaymentReactor]          -> ProcessPaymentCommand -> PaymentAuthorized
                   -> [FulfillmentReactor]      -> ShipOrderCommand      -> OrderShipped
                   -> [OrderReactor]            -> SendNotificationCommand
            """);

        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var dispatcher = new WorkflowCommandDispatcher(transport);
        var correlationId = Guid.NewGuid().ToString();

        // Step 1: InventoryEventProcessor - OrderPlaced -> InventoryReserved
        var inventoryProcessor = new InventoryEventProcessor();
        _ = transport.SubscribeAsync<OrderPlaced>(async (e, ctx, ct) =>
        {
            Console.WriteLine($"\n  [Step 1] InventoryEventProcessor received OrderPlaced(orderId={e.OrderId})");
            var derived = await inventoryProcessor.ProcessAsync(e, ctx, ct);
            foreach (var d in derived)
                await transport.PublishEventAsync(d, ctx.CreateChild(), ct);
        });

        // Step 2: PaymentReactor - InventoryReserved -> ProcessPaymentCommand
        var paymentReactor = new PaymentReactor();
        _ = transport.SubscribeAsync<InventoryReserved>(async (e, ctx, ct) =>
        {
            Console.WriteLine($"  [Step 2] PaymentReactor received InventoryReserved(orderId={e.OrderId})");
            var commands = await paymentReactor.ReactAsync(e, ctx, ct);
            foreach (var cmd in commands)
                await dispatcher.DispatchAsync(cmd, ctx.CreateChild(), ct);
        });

        // Step 3: FulfillmentReactor - PaymentAuthorized -> ShipOrderCommand
        var fulfillmentReactor = new FulfillmentReactor();
        _ = transport.SubscribeAsync<PaymentAuthorized>(async (e, ctx, ct) =>
        {
            Console.WriteLine($"  [Step 3] FulfillmentReactor received PaymentAuthorized(orderId={e.OrderId})");
            var commands = await fulfillmentReactor.ReactAsync(e, ctx, ct);
            foreach (var cmd in commands)
                await dispatcher.DispatchAsync(cmd, ctx.CreateChild(), ct);
        });

        // Step 4: OrderReactor - OrderShipped -> SendNotificationCommand
        var orderReactor = new OrderReactor();
        var notifications = 0;
        _ = transport.SubscribeAsync<OrderShipped>(async (e, ctx, ct) =>
        {
            Console.WriteLine($"  [Step 4] OrderReactor received OrderShipped(orderId={e.OrderId})");
            var commands = await orderReactor.ReactAsync(e, ctx, ct);
            foreach (var cmd in commands.Cast<SendNotificationCommand>())
            {
                notifications++;
                Console.WriteLine();
                Console.WriteLine("  [ok] SAGA COMPLETE");
                Console.WriteLine("      SendNotificationCommand dispatched");
                Console.WriteLine($"      OrderId:     {cmd.OrderId}");
                Console.WriteLine($"      CustomerId:  {cmd.CustomerId}");
                Console.WriteLine($"      Message:     {cmd.Message}");
                Console.WriteLine($"      CorrelationId: {ctx.CorrelationId}  (constant across all 4 steps)");
            }
        });

        // Trigger the chain
        var order = new OrderPlaced(
            Id: Guid.NewGuid(),
            Timestamp: DateTime.UtcNow,
            OrderId: "order-SAGA-001",
            CustomerId: "cust-42",
            Amount: 249.99m);

        var rootContext = new MessageContext
        {
            MessageId = Guid.NewGuid().ToString(),
            CorrelationId = correlationId,
        };

        Console.WriteLine();
        Console.WriteLine($"  Publishing OrderPlaced(orderId={order.OrderId}) with CorrelationId={correlationId[..8]}...");
        await transport.PublishAsync(order, rootContext);
        if (notifications != 1)
            throw new InvalidOperationException($"Expected the choreography to send 1 notification, got {notifications}.");

        Console.WriteLine();
    }

    // -------------------------------------------------------------------------
    // Part B - DCB in-process. One single-threaded pass over an in-memory log.
    // A crash before the next append leaves the chain short. There is no inbox.
    //
    // What changes vs Part A:
    //   REMOVE  IMessageTransport
    //   REMOVE  IInboxStore
    //   REPLACE IReactor<TEvent> with DcbReactor<TEvent>
    //   REPLACE the transport with an in-memory list this method polls once
    // -------------------------------------------------------------------------

    public static async Task RunDcbAsync()
    {
        Console.WriteLine("--- Part B: DCB In-Process (one pass, no broker) -----------------------");
        Console.WriteLine("""
            Same saga, one pass over an in-memory log. No broker and no inbox.
            A crash before the next append leaves the earlier steps in place.

            What changed vs Part A:
              REMOVED  IMessageTransport
              REMOVED  IInboxStore
              REPLACED IReactor<TEvent> with DcbReactor<TEvent>
            """);

        var log = new InMemoryEventLog();

        // Wire up DCB reactors
        var reactors = new IDcbReactor[]
        {
            new DcbInventoryReactor(),
            new DcbPaymentReactor(),
            new DcbFulfillmentReactor(),
            new DcbNotificationReactor(),
        };

        // Append the initial event (simulates what an aggregate would do)
        var orderId = "order-DCB-001";
        var orderPlaced = new OrderPlaced(
            Id: Guid.NewGuid(),
            Timestamp: DateTime.UtcNow,
            OrderId: orderId,
            CustomerId: "cust-42",
            Amount: 249.99m);

        Console.WriteLine($"\n  Appending OrderPlaced(orderId={orderId}) to event log...");
        log.Append(orderPlaced, tags: [$"order:{orderId}"]);

        // Run the DCB reaction loop - each reactor sees the current log state
        await RunDcbLoopAsync(log, reactors, orderId);

        Console.WriteLine();
    }

    private static async Task RunDcbLoopAsync(InMemoryEventLog log, IDcbReactor[] reactors, string orderId)
    {
        // Simple single-pass loop: each reactor inspects new events and reacts
        // (In production, DcbReactors run as hosted services polling the real event store)
        var processed = new HashSet<Guid>();
        bool anyReaction;

        do
        {
            anyReaction = false;
            var snapshot = log.GetAll();

            foreach (var (evt, tags) in snapshot)
            {
                if (processed.Contains(evt.Id))
                    continue;

                foreach (var reactor in reactors)
                {
                    if (!reactor.GetTagsForReaction().Any(pattern => TagMatches(tags, pattern)))
                        continue;

                    var commands = await reactor.ReactAsync(evt, new EventMetadata(), CancellationToken.None);
                    foreach (var cmd in commands)
                    {
                        anyReaction = true;
                        // Simulate in-memory aggregate handling: command -> new event
                        var newEvents = HandleCommand(cmd, orderId);
                        foreach (var newEvt in newEvents)
                        {
                            Console.WriteLine($"  [DCB] {reactor.GetType().Name} -> {cmd.GetType().Name} -> {newEvt.GetType().Name}");
                            log.Append(newEvt, tags: GetTagsForEvent(newEvt, orderId));
                        }
                    }
                }

                processed.Add(evt.Id);
            }
        } while (anyReaction);

        // Find and display the final command
        var finalNotification = log.GetAll()
            .Select(e => e.Event)
            .OfType<OrderShipped>()
            .FirstOrDefault();

        if (finalNotification is null)
            throw new InvalidOperationException("DCB saga did not append OrderShipped.");

        Console.WriteLine();
        Console.WriteLine("  [ok] SAGA COMPLETE (DCB)");
        Console.WriteLine($"      OrderShipped(orderId={finalNotification.OrderId})");
        Console.WriteLine($"      TrackingNumber: {finalNotification.TrackingNumber}");
        Console.WriteLine("      One pass in this process. No broker and no inbox.");
    }

    private static IEnumerable<IEvent> HandleCommand(ICommand command, string orderId)
    {
        // Simulate aggregate command handling without SQL Server
        return command switch
        {
            ProcessPaymentCommand cmd => [new PaymentAuthorized(
                Guid.NewGuid(), DateTime.UtcNow, cmd.OrderId, cmd.Amount, "123 Main St")],
            ShipOrderCommand cmd => [new OrderShipped(
                Guid.NewGuid(), DateTime.UtcNow, cmd.OrderId, "cust-42", $"TRK-{Guid.NewGuid().ToString()[..8].ToUpper()}")],
            SendNotificationCommand => [],  // terminal - no further events
            _ => [],
        };
    }

    /// <summary>
    /// <paramref name="pattern"/> <c>order:*</c> matches any tag that starts with <c>order:</c>.
    /// A pattern without <c>*</c> must match the tag exactly.
    /// </summary>
    private static bool TagMatches(HashSet<string> tags, string pattern)
    {
        if (pattern.EndsWith(":*", StringComparison.Ordinal))
        {
            var prefix = pattern[..^1];
            return tags.Any(tag => tag.StartsWith(prefix, StringComparison.Ordinal));
        }

        return tags.Contains(pattern);
    }

    private static string[] GetTagsForEvent(IEvent evt, string orderId) => evt switch
    {
        InventoryReserved => [$"order:{orderId}", $"inventory:{orderId}"],
        PaymentAuthorized => [$"order:{orderId}", $"payment:{orderId}"],
        OrderShipped      => [$"order:{orderId}"],
        _                 => [$"order:{orderId}"],
    };

    // -------------------------------------------------------------------------
    // EDA Reactor and Event Processor implementations (Part A)
    // -------------------------------------------------------------------------

    /// <summary>Reacts to <see cref="InventoryReserved"/> -> emits <see cref="ProcessPaymentCommand"/>.</summary>
    private sealed class PaymentReactor : IReactor<InventoryReserved>
    {
        public Task<IEnumerable<ICommand>> ReactAsync(
            InventoryReserved @event,
            MessageContext context,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IEnumerable<ICommand>>(
            [
                new ProcessPaymentCommand(Guid.NewGuid(), @event.OrderId, Amount: 249.99m),
            ]);
    }

    /// <summary>Reacts to <see cref="PaymentAuthorized"/> -> emits <see cref="ShipOrderCommand"/>.</summary>
    private sealed class FulfillmentReactor : IReactor<PaymentAuthorized>
    {
        public Task<IEnumerable<ICommand>> ReactAsync(
            PaymentAuthorized @event,
            MessageContext context,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IEnumerable<ICommand>>(
            [
                new ShipOrderCommand(Guid.NewGuid(), @event.OrderId, @event.ShippingAddress),
            ]);
    }

    /// <summary>
    /// Simulated in-memory command dispatcher used in Part A.
    /// In production this calls <c>IAggregateRepository.HandleCommandAsync</c> and saves real events.
    /// Here it publishes synthetic events directly to the transport so the chain continues.
    /// </summary>
    private sealed class WorkflowCommandDispatcher
    {
        private readonly IMessageTransport _transport;

        public WorkflowCommandDispatcher(IMessageTransport transport)
            => _transport = transport;

        public async Task DispatchAsync(ICommand command, MessageContext context, CancellationToken ct = default)
        {
            // Simulate aggregate handling: command -> next event published back to broker
            IEvent? resultEvent = command switch
            {
                ProcessPaymentCommand cmd =>
                    new PaymentAuthorized(Guid.NewGuid(), DateTime.UtcNow, cmd.OrderId, cmd.Amount, "123 Main St"),
                ShipOrderCommand cmd =>
                    new OrderShipped(Guid.NewGuid(), DateTime.UtcNow, cmd.OrderId, "cust-42",
                        $"TRK-{Guid.NewGuid().ToString()[..8].ToUpper()}"),
                _ => null,
            };

            if (resultEvent is not null)
            {
                Console.WriteLine($"      [WorkflowDispatcher] {command.GetType().Name} -> {resultEvent.GetType().Name}");
                await _transport.PublishEventAsync(resultEvent, context.CreateChild(), ct);
            }
        }
    }

    // -------------------------------------------------------------------------
    // DCB Reactor implementations (Part B)
    // -------------------------------------------------------------------------

    private sealed class DcbInventoryReactor : DcbReactor<OrderPlaced>
    {
        public override string[] GetTagsForReaction() => ["order:*"];

        protected override Task<IEnumerable<ICommand>> ReactToEventAsync(
            OrderPlaced @event, EventMetadata metadata, CancellationToken ct = default) =>
            Task.FromResult<IEnumerable<ICommand>>(
            [
                // In production: check if inventory already reserved before emitting
                new ProcessPaymentCommand(Guid.NewGuid(), @event.OrderId, @event.Amount),
            ]);
    }

    private sealed class DcbPaymentReactor : DcbReactor<InventoryReserved>
    {
        public override string[] GetTagsForReaction() => ["inventory:*"];

        protected override Task<IEnumerable<ICommand>> ReactToEventAsync(
            InventoryReserved @event, EventMetadata metadata, CancellationToken ct = default) =>
            Task.FromResult<IEnumerable<ICommand>>(
            [
                new ProcessPaymentCommand(Guid.NewGuid(), @event.OrderId, Amount: 249.99m),
            ]);
    }

    private sealed class DcbFulfillmentReactor : DcbReactor<PaymentAuthorized>
    {
        public override string[] GetTagsForReaction() => ["payment:*"];

        protected override Task<IEnumerable<ICommand>> ReactToEventAsync(
            PaymentAuthorized @event, EventMetadata metadata, CancellationToken ct = default) =>
            Task.FromResult<IEnumerable<ICommand>>(
            [
                new ShipOrderCommand(Guid.NewGuid(), @event.OrderId, @event.ShippingAddress),
            ]);
    }

    private sealed class DcbNotificationReactor : DcbReactor<OrderShipped>
    {
        public override string[] GetTagsForReaction() => ["order:*"];

        protected override Task<IEnumerable<ICommand>> ReactToEventAsync(
            OrderShipped @event, EventMetadata metadata, CancellationToken ct = default) =>
            Task.FromResult<IEnumerable<ICommand>>(
            [
                new SendNotificationCommand(
                    Guid.NewGuid(), @event.OrderId, @event.CustomerId,
                    $"Your order {@event.OrderId} has shipped! Track: {@event.TrackingNumber}"),
            ]);
    }

    // -------------------------------------------------------------------------
    // Lightweight in-memory event log for the DCB demo
    // Replaces SqlServerEventStore - keeps the demo self-contained under dotnet run eda
    // -------------------------------------------------------------------------

    private sealed class InMemoryEventLog
    {
        private readonly List<(IEvent Event, HashSet<string> Tags)> _entries = [];

        public void Append(IEvent evt, string[] tags)
            => _entries.Add((evt, [.. tags]));

        public IReadOnlyList<(IEvent Event, HashSet<string> Tags)> GetAll()
            => _entries.ToArray();
    }
}
