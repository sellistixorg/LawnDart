using Microsoft.Extensions.Options;
using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Testing.Bdd;

/// <summary>
/// Shared context used by BDD test helpers.
/// Creates an in-memory event store and repository adapters.
/// </summary>
public sealed class BddTestContext : IAsyncDisposable
{
    public IEventStore EventStore { get; }
    public IAggregateRepository AggregateRepository { get; }
    public IDcbRepository DcbRepository { get; }

    private BddTestContext(
        IEventStore eventStore,
        IAggregateRepository aggregateRepository,
        IDcbRepository dcbRepository)
    {
        EventStore = eventStore;
        AggregateRepository = aggregateRepository;
        DcbRepository = dcbRepository;
    }

    /// <summary>
    /// Creates a fresh InMemory-backed test context. Primary GWT host.
    /// </summary>
    /// <remarks>
    /// The in-memory store does not require an event-type catalog.
    /// </remarks>
    public static BddTestContext CreateInMemory()
    {
        var store = new InMemoryEventStore();

        var tenantProvider = new NullTenantContextProvider();
        var metadataProvider = new DefaultMetadataProvider();
        var lawnDartOptions = Options.Create(new LawnDartOptions
        {
            RequireTenantId = false,
            EnableAuthorization = false
        });

        var eventSourcingOptions = Options.Create(new EventSourcingOptions
        {
            EnforceDcbTenantIsolation = false
        });

        var aggregateRepository = new AggregateRepository(
            store,
            metadataProvider,
            tenantProvider,
            lawnDartOptions);

        var dcbRepository = new DcbRepository(
            store,
            metadataProvider,
            tenantProvider,
            lawnDartOptions,
            eventSourcingOptions: eventSourcingOptions);

        return new BddTestContext(store, aggregateRepository, dcbRepository);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private sealed class NullTenantContextProvider : ITenantContextProvider
    {
        public string? GetTenantId() => null;

        public string GetTenantIdRequired()
            => throw new InvalidOperationException("No tenant is configured for this BDD test context.");
    }
}
