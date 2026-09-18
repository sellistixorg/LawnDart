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
    /// <param name="eventTypes">
    /// Event types to materialize into this spec's catalog. Required for typed
    /// hydrate of Given events and emitted events.
    /// </param>
    public static BddTestContext CreateInMemory(params Type[] eventTypes)
        => Create(new InMemoryEventStore(
            session: new EventSession(
                new LawnDart.EventSourcing.Serialization.JsonEventSerializer(),
                EventTypeCatalog.Materialize(eventTypes))));

    /// <summary>
    /// Builds a context against <paramref name="store"/>. Used by tests that
    /// need a store with a missing capability or a broken probe.
    /// </summary>
    internal static BddTestContext Create(IEventStore store)
    {
        ArgumentNullException.ThrowIfNull(store);

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
