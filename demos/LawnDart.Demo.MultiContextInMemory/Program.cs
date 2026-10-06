// Multi-context InMemory demo.
//
// Two bounded contexts ("ordering" and "catalog") in one host. Each context
// has its own InMemory event store.
//
// What the run shows:
//   1. Each context has its own IEventStore instance.
//   2. Global sequence counters are per context. Appending to one leaves the other unchanged.
//   3. Handlers inject IEventStore with no context key. ContextServiceProvider supplies the store.
//   4. ContextAwareCommandDispatcher routes each command through ICommandContextRegistry.
//   5. IBoundedContextRegistry lists the registered contexts.
//
// Run:
//   dotnet run --project demos/LawnDart.Demo.MultiContextInMemory

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart;
using LawnDart.Demo.MultiContextInMemory.Catalog;
using LawnDart.Demo.MultiContextInMemory.Ordering;
using LawnDart.EventSourcing;
using LawnDart.EventStore;
using LawnDart.Messaging;

namespace LawnDart.Demo.MultiContextInMemory;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        builder.Logging.SetMinimumLevel(LogLevel.Information);
        builder.Logging.AddSimpleConsole(c => c.TimestampFormat = "[HH:mm:ss] ");

        var services = builder.Services;

        services.AddLawnDart(o => o.RequireTenantId = false);

        // Both contexts live in this assembly. Pass each context's own types.
        // An assembly scan would register every handler and event on every context.
        services.AddBoundedContext("ordering")
            .UseInMemory()
            .WithEventTypes(typeof(OrderPlaced), typeof(OrderFulfilled))
            .WithCommandHandlers([
                typeof(PlaceOrderHandler),
                typeof(FulfillOrderHandler)]);

        services.AddBoundedContext("catalog")
            .UseInMemory()
            .WithEventTypes(typeof(ProductPublished), typeof(ProductRetired))
            .WithCommandHandlers([
                typeof(PublishProductHandler),
                typeof(RetireProductHandler)]);

        services.AddHostedService<DemoRunner>();

        var host = builder.Build();
        await host.RunAsync();
    }
}

/// <summary>
/// Runs the demo once, then stops the host.
/// </summary>
internal sealed class DemoRunner : IHostedService
{
    private readonly ICommandDispatcher _dispatcher;
    private readonly IBoundedContextRegistry _contextRegistry;
    private readonly IEventStore _orderingStore;
    private readonly IEventStore _catalogStore;
    private readonly ILogger<DemoRunner> _logger;
    private readonly IHostApplicationLifetime _lifetime;

    public DemoRunner(
        ICommandDispatcher dispatcher,
        IBoundedContextRegistry contextRegistry,
        IServiceProvider sp,
        ILogger<DemoRunner> logger,
        IHostApplicationLifetime lifetime)
    {
        _dispatcher = dispatcher;
        _contextRegistry = contextRegistry;
        _orderingStore = sp.GetRequiredKeyedService<IEventStore>("ordering");
        _catalogStore = sp.GetRequiredKeyedService<IEventStore>("catalog");
        _logger = logger;
        _lifetime = lifetime;
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("=== Multi-Context InMemory Demo ===");
        _logger.LogInformation("Registered bounded contexts ({Count}): {Names}",
            _contextRegistry.ContextNames.Count,
            string.Join(", ", _contextRegistry.ContextNames));
        _logger.LogInformation("Multi-context mode: {IsMulti}", _contextRegistry.IsMultiContext);

        _logger.LogInformation("");
        _logger.LogInformation("--- Ordering Context ---");

        var order1Id = Guid.NewGuid();
        await _dispatcher.DispatchAsync(
            new PlaceOrderCommand(order1Id, "customer-001", ["item-A", "item-B"]),
            new MessageContext(), cancellationToken);

        await _dispatcher.DispatchAsync(
            new FulfillOrderCommand(order1Id),
            new MessageContext(), cancellationToken);

        var order2Id = Guid.NewGuid();
        await _dispatcher.DispatchAsync(
            new PlaceOrderCommand(order2Id, "customer-002", ["item-C"]),
            new MessageContext(), cancellationToken);

        var orderingSeq = await _orderingStore.GetCurrentSequenceAsync(cancellationToken);
        _logger.LogInformation("Ordering context global sequence after 3 appends: {Seq}", orderingSeq);

        _logger.LogInformation("");
        _logger.LogInformation("--- Catalog Context ---");

        var productId = Guid.NewGuid();
        await _dispatcher.DispatchAsync(
            new PublishProductCommand(productId, "Widget Pro", 29.99m),
            new MessageContext(), cancellationToken);

        await _dispatcher.DispatchAsync(
            new RetireProductCommand(productId),
            new MessageContext(), cancellationToken);

        var catalogSeq = await _catalogStore.GetCurrentSequenceAsync(cancellationToken);
        _logger.LogInformation("Catalog context global sequence after 2 appends: {Seq}", catalogSeq);

        _logger.LogInformation("");
        _logger.LogInformation("--- Isolation Verification ---");
        _logger.LogInformation(
            "Ordering events: {Seq}. Catalog events: {CatSeq}",
            orderingSeq, catalogSeq);
        var storesDiffer = !ReferenceEquals(_orderingStore, _catalogStore);
        _logger.LogInformation("Stores are independent: {Independent}", storesDiffer);

        _logger.LogInformation("");
        _logger.LogInformation("--- Reading Ordering Events for Order {OrderId} ---", order1Id);
        var orderStream = $"Order:{order1Id}";
        var orderEvents = await _orderingStore.ReadStreamAsync(orderStream, cancellationToken: cancellationToken);
        foreach (var e in orderEvents)
            _logger.LogInformation("  [{Seq}] v{Version} {EventType}", e.SequencePosition, e.Version, e.Event.GetType().Name);

        if (orderingSeq != 3 || catalogSeq != 2 || !storesDiffer || orderEvents.Count != 2)
        {
            _logger.LogError(
                "Isolation check failed. Expected ordering sequence 3, catalog sequence 2, distinct stores, and 2 events on the first order.");
            Environment.ExitCode = 1;
        }

        _logger.LogInformation("");
        _logger.LogInformation("=== Demo complete ===");

        _lifetime.StopApplication();
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
