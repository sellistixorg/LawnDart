using Microsoft.Extensions.DependencyInjection;
using LawnDart.Aggregates;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Metadata;
using LawnDart.TestUtilities;

namespace LawnDart.EventSourcing.Tests.BoundedContext;

/// <summary>
/// End-to-end integration test covering two independent InMemory bounded contexts
/// running in the same application.
/// </summary>
public class MultiContextInMemoryIntegrationTests
{
    private static IServiceProvider BuildProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });

        services.AddBoundedContext("ordering")
            .UseInMemory()
            .WithCommandHandlers([typeof(OrderingHandler)]);

        services.AddBoundedContext("catalog")
            .UseInMemory()
            .WithCommandHandlers([typeof(CatalogHandler)]);

        return services.BuildServiceProvider();
    }

    [Fact]
    public void TwoContexts_ReturnedFromIBoundedContextEventStoreEnumerable()
    {
        var sp       = BuildProvider();
        var contexts = sp.GetServices<IBoundedContextEventStore>().ToList();

        Assert.Equal(2, contexts.Count);
        Assert.Contains(contexts, c => c.ContextName == "ordering");
        Assert.Contains(contexts, c => c.ContextName == "catalog");
    }

    [Fact]
    public void TwoContexts_HavePhysicallyIsolatedEventStores()
    {
        var sp       = BuildProvider();
        var ordering = sp.GetRequiredKeyedService<IEventStore>("ordering");
        var catalog  = sp.GetRequiredKeyedService<IEventStore>("catalog");

        Assert.NotSame(ordering, catalog);
    }

    [Fact]
    public async Task TwoContexts_GlobalSequencesAreIndependent()
    {
        var sp       = BuildProvider();
        var ordering = sp.GetRequiredKeyedService<IEventStore>("ordering");
        var catalog  = sp.GetRequiredKeyedService<IEventStore>("catalog");

        // Append 3 events to ordering, 1 to catalog
        await ordering.AppendAsync("Order:1", [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        await ordering.AppendAsync("Order:2", [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        await ordering.AppendAsync("Order:3", [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        await catalog.AppendAsync("Product:1", [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        Assert.Equal(3, await ordering.GetCurrentSequenceAsync());
        Assert.Equal(1, await catalog.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task Dispatcher_RoutesOrderCommandToOrderingStore()
    {
        var sp         = BuildProvider();
        var dispatcher = sp.GetRequiredService<ICommandDispatcher>();
        var ordering   = sp.GetRequiredKeyedService<IEventStore>("ordering");
        var catalog    = sp.GetRequiredKeyedService<IEventStore>("catalog");

        await dispatcher.DispatchAsync(
            new PlaceOrderCommand(Guid.NewGuid(), "order-stream"),
            new MessageContext(),
            CancellationToken.None);

        Assert.Equal(1, await ordering.GetCurrentSequenceAsync());
        Assert.Equal(0, await catalog.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task Dispatcher_RoutesCatalogCommandToCatalogStore()
    {
        var sp         = BuildProvider();
        var dispatcher = sp.GetRequiredService<ICommandDispatcher>();
        var ordering   = sp.GetRequiredKeyedService<IEventStore>("ordering");
        var catalog    = sp.GetRequiredKeyedService<IEventStore>("catalog");

        await dispatcher.DispatchAsync(
            new PublishCatalogItemCommand(Guid.NewGuid(), "product-stream"),
            new MessageContext(),
            CancellationToken.None);

        Assert.Equal(0, await ordering.GetCurrentSequenceAsync());
        Assert.Equal(1, await catalog.GetCurrentSequenceAsync());
    }

    [Fact]
    public void IBoundedContextRegistry_ReflectsMultiContext()
    {
        var sp       = BuildProvider();
        var registry = sp.GetRequiredService<IBoundedContextRegistry>();

        Assert.True(registry.IsMultiContext);
        Assert.Equal(2, registry.ContextNames.Count);
    }

    // ── Commands ───────────────────────────────────────────────────────────────

    public sealed record PlaceOrderCommand(Guid Id, string Stream) : ICommand;
    public sealed record PublishCatalogItemCommand(Guid Id, string Stream) : ICommand;

    // ── Handlers ──────────────────────────────────────────────────────────────

    public sealed class OrderingHandler : ICommandHandler<PlaceOrderCommand>
    {
        private readonly IEventStore _eventStore;

        public OrderingHandler(IEventStore eventStore) => _eventStore = eventStore;

        public async Task HandleAsync(PlaceOrderCommand cmd, CancellationToken ct = default)
        {
            await _eventStore.AppendAsync(cmd.Stream, [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)], cancellationToken: ct);
        }
    }

    public sealed class CatalogHandler : ICommandHandler<PublishCatalogItemCommand>
    {
        private readonly IEventStore _eventStore;

        public CatalogHandler(IEventStore eventStore) => _eventStore = eventStore;

        public async Task HandleAsync(PublishCatalogItemCommand cmd, CancellationToken ct = default)
        {
            await _eventStore.AppendAsync(cmd.Stream, [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)], cancellationToken: ct);
        }
    }

    // ── Stubs ──────────────────────────────────────────────────────────────────

    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}
