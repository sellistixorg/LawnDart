using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Authorization;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.Snapshots;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Snapshots;
using LawnDart.Tagging;

namespace LawnDart.EventSourcing;

/// <summary>
/// Builds repositories that enqueue snapshot writes when a write queue is registered.
/// Third-party stores call these instead of the public constructors, which omit the
/// queue and skip snapshot writes.
/// </summary>
/// <remarks>
/// <c>UseInMemory</c> and <c>UseSqlServer</c> already use this factory. A custom
/// <c>Use*</c> method should call <see cref="BoundedContextBuilderExtensions.AddSnapshotWriteInfrastructure"/>
/// and construct repositories here so <c>.WithSnapshots()</c> keeps writing.
/// </remarks>
public static class EventSourcingRepositories
{
    /// <summary>
    /// Creates an <see cref="IAggregateRepository"/> for <paramref name="serviceKey"/>,
    /// passing the snapshot write queue when one is registered.
    /// </summary>
    /// <param name="services">The application service provider.</param>
    /// <param name="serviceKey">The bounded-context name used as the keyed-service key.</param>
    /// <returns>A repository wired to the keyed store and the shared write queue.</returns>
    public static IAggregateRepository CreateAggregateRepository(
        IServiceProvider services,
        object serviceKey)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(serviceKey);

        return new AggregateRepository(
            services.GetRequiredKeyedService<IEventStore>(serviceKey),
            services.GetRequiredService<IMetadataProvider>(),
            services.GetRequiredService<ITenantContextProvider>(),
            services.GetRequiredService<IOptions<LawnDartOptions>>(),
            services.GetKeyedService<ITagProvider>(serviceKey) ?? services.GetService<ITagProvider>(),
            services.GetService<AuthorizationService>(),
            services.GetService<ILogger<AggregateRepository>>(),
            services.GetKeyedService<ISnapshotStore>(serviceKey) ?? services.GetService<ISnapshotStore>(),
            services.GetService<ISnapshotStrategyResolver>(),
            services.GetService<ISnapshotWriteQueue>());
    }

    /// <summary>
    /// Creates an <see cref="IDcbRepository"/> for <paramref name="serviceKey"/>,
    /// passing the snapshot write queue when one is registered.
    /// </summary>
    /// <param name="services">The application service provider.</param>
    /// <param name="serviceKey">The bounded-context name used as the keyed-service key.</param>
    /// <returns>A repository wired to the keyed store and the shared write queue.</returns>
    public static IDcbRepository CreateDcbRepository(
        IServiceProvider services,
        object serviceKey)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(serviceKey);

        return new DcbRepository(
            services.GetRequiredKeyedService<IEventStore>(serviceKey),
            services.GetRequiredService<IMetadataProvider>(),
            services.GetRequiredService<ITenantContextProvider>(),
            services.GetRequiredService<IOptions<LawnDartOptions>>(),
            services.GetKeyedService<ITagProvider>(serviceKey) ?? services.GetService<ITagProvider>(),
            services.GetService<AuthorizationService>(),
            services.GetService<ILogger<DcbRepository>>(),
            services.GetService<IOptions<EventSourcingOptions>>(),
            services.GetKeyedService<IDcbSnapshotStore>(serviceKey) ?? services.GetService<IDcbSnapshotStore>(),
            services.GetService<ISnapshotStrategyResolver>(),
            services.GetService<ISnapshotWriteQueue>());
    }
}
