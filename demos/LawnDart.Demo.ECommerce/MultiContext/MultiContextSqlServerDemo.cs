using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Messaging;

namespace LawnDart.Demo.ECommerce.MultiContext;

// =============================================================================
// Commands - ordering context
// =============================================================================

internal sealed record PlaceOrderCommand(Guid Id, Guid OrderId, string CustomerId, decimal Total) : ICommand;
internal sealed record ShipOrderCommand(Guid Id, Guid OrderId, string TrackingNumber) : ICommand;

// =============================================================================
// Commands - catalog context
// =============================================================================

internal sealed record ListProductCommand(Guid Id, Guid ProductId, string Name, decimal Price) : ICommand;
internal sealed record DiscontinueProductCommand(Guid Id, Guid ProductId, string Reason) : ICommand;

// =============================================================================
// Events - ordering context
// =============================================================================

[EventTypeName("order-placed-event")]
internal sealed record OrderPlacedEvent(Guid Id, DateTime Timestamp, Guid OrderId, string CustomerId, decimal Total)
    : IEvent;

[EventTypeName("multicontext-order-shipped-event")]
internal sealed record OrderShippedEvent(Guid Id, DateTime Timestamp, Guid OrderId, string TrackingNumber)
    : IEvent;

// =============================================================================
// Events - catalog context
// =============================================================================

[EventTypeName("product-listed-event")]
internal sealed record ProductListedEvent(Guid Id, DateTime Timestamp, Guid ProductId, string Name, decimal Price)
    : IEvent;

[EventTypeName("product-discontinued-event")]
internal sealed record ProductDiscontinuedEvent(Guid Id, DateTime Timestamp, Guid ProductId, string Reason)
    : IEvent;

// =============================================================================
// Command Handlers - ordering context
// IEventStore is injected WITHOUT a context key - ContextServiceProvider resolves
// the correct keyed instance transparently.
// =============================================================================

internal sealed class PlaceOrderHandler : ICommandHandler<PlaceOrderCommand>
{
    private readonly IEventStore _eventStore;
    private readonly ILogger<PlaceOrderHandler> _logger;

    public PlaceOrderHandler(IEventStore eventStore, ILogger<PlaceOrderHandler> logger)
    {
        _eventStore = eventStore;
        _logger     = logger;
    }

    public async Task HandleAsync(PlaceOrderCommand command, CancellationToken ct = default)
    {
        var streamId = $"Order:{command.OrderId}";
        var evt = new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, command.OrderId, command.CustomerId, command.Total);
        await _eventStore.AppendAsync(streamId, [evt], expectedVersion: null, cancellationToken: ct);
        _logger.LogInformation("  [ordering] Order {OrderId} placed for {Customer}", command.Id, command.CustomerId);
    }
}

internal sealed class ShipOrderHandler : ICommandHandler<ShipOrderCommand>
{
    private readonly IEventStore _eventStore;
    private readonly ILogger<ShipOrderHandler> _logger;

    public ShipOrderHandler(IEventStore eventStore, ILogger<ShipOrderHandler> logger)
    {
        _eventStore = eventStore;
        _logger     = logger;
    }

    public async Task HandleAsync(ShipOrderCommand command, CancellationToken ct = default)
    {
        var streamId = $"Order:{command.OrderId}";
        var existing = await _eventStore.ReadStreamAsync(streamId, cancellationToken: ct);
        if (existing.Count == 0)
            throw new InvalidOperationException($"No order stream {streamId}. Place the order before shipping.");
        var evt = new OrderShippedEvent(Guid.NewGuid(), DateTime.UtcNow, command.OrderId, command.TrackingNumber);
        await _eventStore.AppendAsync(streamId, [evt], existing[^1].Version, cancellationToken: ct);
        _logger.LogInformation("  [ordering] Order {OrderId} shipped - tracking: {Tracking}", command.OrderId, command.TrackingNumber);
    }
}

// =============================================================================
// Command Handlers - catalog context
// =============================================================================

internal sealed class ListProductHandler : ICommandHandler<ListProductCommand>
{
    private readonly IEventStore _eventStore;
    private readonly ILogger<ListProductHandler> _logger;

    public ListProductHandler(IEventStore eventStore, ILogger<ListProductHandler> logger)
    {
        _eventStore = eventStore;
        _logger     = logger;
    }

    public async Task HandleAsync(ListProductCommand command, CancellationToken ct = default)
    {
        var streamId = $"Product:{command.ProductId}";
        var evt = new ProductListedEvent(Guid.NewGuid(), DateTime.UtcNow, command.ProductId, command.Name, command.Price);
        await _eventStore.AppendAsync(streamId, [evt], expectedVersion: null, cancellationToken: ct);
        _logger.LogInformation("  [catalog] Product '{Name}' listed at ${Price}", command.Name, command.Price);
    }
}

internal sealed class DiscontinueProductHandler : ICommandHandler<DiscontinueProductCommand>
{
    private readonly IEventStore _eventStore;
    private readonly ILogger<DiscontinueProductHandler> _logger;

    public DiscontinueProductHandler(IEventStore eventStore, ILogger<DiscontinueProductHandler> logger)
    {
        _eventStore = eventStore;
        _logger     = logger;
    }

    public async Task HandleAsync(DiscontinueProductCommand command, CancellationToken ct = default)
    {
        var streamId = $"Product:{command.ProductId}";
        var existing = await _eventStore.ReadStreamAsync(streamId, cancellationToken: ct);
        if (existing.Count == 0)
            throw new InvalidOperationException($"No product stream {streamId}. List the product before discontinuing it.");
        var evt = new ProductDiscontinuedEvent(Guid.NewGuid(), DateTime.UtcNow, command.ProductId, command.Reason);
        await _eventStore.AppendAsync(streamId, [evt], existing[^1].Version, cancellationToken: ct);
        _logger.LogInformation("  [catalog] Product {ProductId} discontinued", command.Id);
    }
}

// =============================================================================
// Demo runner (IHostedService)
// =============================================================================

internal sealed class SqlServerMultiContextDemoRunner : IHostedService
{
    private readonly ICommandDispatcher         _dispatcher;
    private readonly IBoundedContextRegistry    _contextRegistry;
    private readonly IServiceProvider           _sp;
    private readonly ILogger<SqlServerMultiContextDemoRunner> _logger;
    private readonly IHostApplicationLifetime   _lifetime;

    public SqlServerMultiContextDemoRunner(
        ICommandDispatcher dispatcher,
        IBoundedContextRegistry contextRegistry,
        IServiceProvider sp,
        ILogger<SqlServerMultiContextDemoRunner> logger,
        IHostApplicationLifetime lifetime)
    {
        _dispatcher      = dispatcher;
        _contextRegistry = contextRegistry;
        _sp              = sp;
        _logger          = logger;
        _lifetime        = lifetime;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var ctx = new MessageContext();

        _logger.LogInformation("=== SQL Server Multi-Context Demo ===");
        _logger.LogInformation("Registered contexts: {Contexts}",
            string.Join(", ", _contextRegistry.ContextNames));

        // -----------------------------------------------------------------------
        // Commands are dispatched without any context key.
        // ContextAwareCommandDispatcher + ICommandContextRegistry route each
        // command to the correct context's handler automatically.
        // -----------------------------------------------------------------------
        _logger.LogInformation("\n--- Placing orders (ordering -> [ordering] schema) ---");
        var orderId1 = Guid.NewGuid();
        var orderId2 = Guid.NewGuid();
        await _dispatcher.DispatchAsync(new PlaceOrderCommand(Guid.NewGuid(), orderId1, "customer-A", 149.99m), ctx, ct);
        await _dispatcher.DispatchAsync(new PlaceOrderCommand(Guid.NewGuid(), orderId2, "customer-B", 299.00m), ctx, ct);
        await _dispatcher.DispatchAsync(new ShipOrderCommand(Guid.NewGuid(), orderId1, "TRACK-001"), ctx, ct);

        _logger.LogInformation("\n--- Listing products (catalog -> [catalog] schema) ---");
        var productId1 = Guid.NewGuid();
        var productId2 = Guid.NewGuid();
        await _dispatcher.DispatchAsync(new ListProductCommand(Guid.NewGuid(), productId1, "Widget Pro", 29.99m), ctx, ct);
        await _dispatcher.DispatchAsync(new ListProductCommand(Guid.NewGuid(), productId2, "Gadget Plus", 59.99m), ctx, ct);
        await _dispatcher.DispatchAsync(new DiscontinueProductCommand(Guid.NewGuid(), productId2, "End of life"), ctx, ct);

        // -----------------------------------------------------------------------
        // Verify physical isolation
        // -----------------------------------------------------------------------
        _logger.LogInformation("\n--- Verifying physical isolation ---");
        var orderingStore = _sp.GetRequiredKeyedService<IEventStore>("ordering");
        var catalogStore  = _sp.GetRequiredKeyedService<IEventStore>("catalog");

        var orderingResult = await orderingStore.ReadByQueryAsync(Query.All(), cancellationToken: ct);
        var catalogResult  = await catalogStore.ReadByQueryAsync(Query.All(),  cancellationToken: ct);

        _logger.LogInformation("  Events in ordering: {Count}", orderingResult.Events.Count);
        _logger.LogInformation("  Events in catalog: {Count}", catalogResult.Events.Count);
        if (orderingResult.Events.Count != 3 || catalogResult.Events.Count != 3)
        {
            throw new InvalidOperationException(
                $"Expected 3 events in each context. ordering={orderingResult.Events.Count}, catalog={catalogResult.Events.Count}.");
        }

        if (ReferenceEquals(orderingStore, catalogStore))
            throw new InvalidOperationException("ordering and catalog resolved the same event store.");

        // Verify independent sequences
        var orderSeq   = await orderingStore.GetCurrentSequenceAsync(ct);
        var catalogSeq = await catalogStore.GetCurrentSequenceAsync(ct);
        _logger.LogInformation("  [ordering] global sequence position: {Seq}", orderSeq);
        _logger.LogInformation("  [catalog]  global sequence position: {Seq}", catalogSeq);

        _logger.LogInformation("\n=== Multi-Context SQL Server Demo Complete ===");
        _logger.LogInformation("Note: Cross-context queries intentionally absent - contexts are isolated.");

        _lifetime.StopApplication();
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}

// =============================================================================
// Demo entry point (callable from Program.cs)
// =============================================================================

/// <summary>
/// Two bounded contexts, ordering and catalog, each with its own store.
/// InMemory when <paramref name="connectionString"/> is null. SQL Server uses
/// schemas <c>ordering</c> and <c>catalog</c> in that database.
/// </summary>
public static class MultiContextDemo
{
    public static async Task RunAsync(string? connectionString)
    {
        var useSql = !string.IsNullOrWhiteSpace(connectionString);
        Console.WriteLine(useSql
            ? "Multi-context: SQL Server schemas ordering and catalog."
            : "Multi-context: two InMemory stores.");

        var host = Host.CreateDefaultBuilder()
            .ConfigureLogging(log =>
            {
                log.ClearProviders();
                log.AddConsole();
                log.SetMinimumLevel(LogLevel.Warning);
            })
            .ConfigureServices(services =>
            {
                services.AddLawnDart(o => o.RequireTenantId = false);

                var ordering = services.AddBoundedContext("ordering");
                if (useSql)
                {
                    ordering.UseSqlServer(opt =>
                    {
                        opt.ConnectionString = connectionString!;
                        opt.SchemaName = "ordering";
                        opt.RequireTenantId = false;
                    });
                }
                else
                {
                    ordering.UseInMemory();
                }

                ordering
                    .WithEventTypes(typeof(OrderPlacedEvent), typeof(OrderShippedEvent))
                    .WithCommandHandlers([typeof(PlaceOrderHandler), typeof(ShipOrderHandler)]);

                var catalog = services.AddBoundedContext("catalog");
                if (useSql)
                {
                    catalog.UseSqlServer(opt =>
                    {
                        opt.ConnectionString = connectionString!;
                        opt.SchemaName = "catalog";
                        opt.RequireTenantId = false;
                    });
                }
                else
                {
                    catalog.UseInMemory();
                }

                catalog
                    .WithEventTypes(typeof(ProductListedEvent), typeof(ProductDiscontinuedEvent))
                    .WithCommandHandlers([typeof(ListProductHandler), typeof(DiscontinueProductHandler)]);

                services.AddHostedService<SqlServerMultiContextDemoRunner>();
            })
            .Build();

        if (useSql)
        {
            var orderingStore = host.Services.GetRequiredKeyedService<IEventStore>("ordering");
            var catalogStore = host.Services.GetRequiredKeyedService<IEventStore>("catalog");
            if (orderingStore is not SqlServerEventStore orderingSql || catalogStore is not SqlServerEventStore catalogSql)
                throw new InvalidOperationException("SQL multi-context did not resolve SqlServerEventStore.");
            await orderingSql.InitializeSchemaAsync();
            await catalogSql.InitializeSchemaAsync();
        }

        await host.RunAsync();
    }
}
