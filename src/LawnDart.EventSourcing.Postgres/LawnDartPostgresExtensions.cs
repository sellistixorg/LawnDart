using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.Outbox;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.Postgres.Outbox;
using LawnDart.EventSourcing.Postgres.Snapshots;
using LawnDart.EventSourcing.Serialization;
using LawnDart.EventStore;
using LawnDart.Outbox;
using LawnDart.Serialization;
using LawnDart.Snapshots;
using LawnDart.Tagging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.EventSourcing.Postgres;

/// <summary>
/// Registers the Postgres event store on a bounded context.
/// </summary>
public static class LawnDartPostgresExtensions
{
    /// <summary>
    /// Configures this bounded context to use the Postgres event store.
    /// Tables live in <see cref="PostgresEventStoreOptions.SchemaName"/>.
    /// The <c>default</c> context uses <c>lawndart</c>. Any other context uses its own name.
    /// The <c>public</c> schema is rejected.
    /// </summary>
    /// <remarks>
    /// Keyed services use <see cref="BoundedContextBuilder.ContextName"/>.
    /// The context named <c>default</c> also gets unkeyed aliases.
    /// The callback must set <see cref="PostgresEventStoreOptions.ConnectionString"/>.
    /// <see cref="PostgresEventStoreOptions.EnableOutbox"/> writes outbox rows in the append.
    /// Call <c>InitializeSchemaAsync</c> from a deploy or admin step so the outbox table
    /// exists before the first append. Normal app startup does not call it.
    /// </remarks>
    public static BoundedContextBuilder UsePostgres(
        this BoundedContextBuilder builder,
        Action<PostgresEventStoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var services = builder.Services;
        var contextName = builder.ContextName;
        var options = new PostgresEventStoreOptions
        {
            ContextName = contextName,
            SchemaName = string.Equals(contextName, LawnDart.LawnDartNames.DefaultContextName, StringComparison.Ordinal)
                ? "lawndart"
                : contextName,
            ValidateNames = LawnDart.LawnDartNames.IsFormatCheckEnabled(services)
        };
        configure(options);
        options.EnsureValid();

        services.AddKeyedSingleton(contextName, LawnDart.EventSourcing.Context.EventStoreContextMarker.Instance);
        services.TryAddSingleton<IEventSerializer, JsonEventSerializer>();

        services.AddKeyedSingleton<IOutboxWriter>(contextName, (sp, _) =>
            new PostgresOutboxWriter(
                options.ConnectionString!,
                options.OutboxTableName,
                sp.GetService<ILogger<PostgresOutboxWriter>>(),
                options.SchemaName,
                options.OutboxClaimLease,
                options.MigrationConnectionString));

        services.AddKeyedSingleton<IEventStore>(contextName, (sp, _) =>
        {
            var serializer = sp.GetRequiredService<IEventSerializer>();
            var catalog = sp.GetKeyedService<IEventTypeCatalog>(contextName)
                ?? throw new InvalidOperationException(BoundedContextExtensions.MissingEventTypesMessage(contextName));
            var pipeline = sp.GetKeyedService<EventUpcastPipeline>(contextName);
            var session = new EventSession(serializer, catalog, pipeline);
            return new PostgresEventStore(
                options.ConnectionString!,
                serializer,
                options,
                sp.GetService<ILogger<PostgresEventStore>>(),
                session);
        });

        services.AddKeyedSingleton<IEventLog>(contextName,
            (sp, key) => (IEventLog)sp.GetRequiredKeyedService<IEventStore>(key!));
        services.AddKeyedSingleton<IStreamRegistry>(contextName,
            (sp, key) => (IStreamRegistry)sp.GetRequiredKeyedService<IEventStore>(key!));
        services.AddKeyedSingleton<IEventStoreSubscriptions>(contextName,
            (sp, key) => (IEventStoreSubscriptions)sp.GetRequiredKeyedService<IEventStore>(key!));
        services.AddKeyedTransient<IAggregateRepository>(contextName,
            (sp, key) => EventSourcingRepositories.CreateAggregateRepository(sp, key!));
        services.AddKeyedTransient<IDcbRepository>(contextName,
            (sp, key) => EventSourcingRepositories.CreateDcbRepository(sp, key!));

        BoundedContextBuilderExtensions.AddSnapshotWriteInfrastructure(services);

        if (string.Equals(contextName, LawnDart.LawnDartNames.DefaultContextName, StringComparison.Ordinal))
        {
            services.TryAddSingleton<IEventStore>(sp => sp.GetRequiredKeyedService<IEventStore>("default"));
            services.TryAddSingleton<IEventLog>(sp => sp.GetRequiredKeyedService<IEventLog>("default"));
            services.TryAddSingleton<IStreamRegistry>(sp => sp.GetRequiredKeyedService<IStreamRegistry>("default"));
            services.TryAddSingleton<IEventStoreSubscriptions>(sp => sp.GetRequiredKeyedService<IEventStoreSubscriptions>("default"));
            services.TryAddTransient<IAggregateRepository>(sp => sp.GetRequiredKeyedService<IAggregateRepository>("default"));
            services.TryAddTransient<IDcbRepository>(sp => sp.GetRequiredKeyedService<IDcbRepository>("default"));
        }

        services.AddSingleton<IBoundedContextEventStore>(sp =>
            new PostgresBoundedContextEventStore(contextName, sp.GetRequiredKeyedService<IEventStore>(contextName)));

        if (options.EnableOutbox)
        {
            services.AddSingleton<IHostedService>(sp =>
            {
                var outboxWriter = sp.GetRequiredKeyedService<IOutboxWriter>(contextName);
                var publishers = sp.GetServices<IOutboxPublisher>().ToArray();
                if (publishers.Length == 0)
                {
                    throw new InvalidOperationException(
                        $"EnableOutbox is true for context '{contextName}' but no IOutboxPublisher is registered.");
                }

                IOutboxPublisher publisher = publishers.Length == 1
                    ? publishers[0]
                    : new CompositeOutboxPublisher(publishers);
                var store = sp.GetRequiredKeyedService<IEventStore>(contextName);
                return new OutboxProcessor(
                    outboxWriter,
                    publisher,
                    sp.GetService<ILogger<OutboxProcessor>>() ?? NullLogger<OutboxProcessor>.Instance,
                    newEvents: (store as ISignalsNewEvents)?.NewEvents);
            });
        }

        services.AddKeyedSingleton(contextName, options);
        services.TryAddKeyedSingleton<UnknownTagWarningLog>(contextName, (_, _) => new UnknownTagWarningLog());
        services.Configure<EventSourcingOptions>(o =>
        {
            if (!options.WarnOnUnknownTag)
                o.WarnOnUnknownTag = false;
            if (options.FormatAggregateFallbackTag)
                o.FormatAggregateFallbackTag = true;
        });

        return builder;
    }

    /// <summary>
    /// Enables Postgres snapshot storage for stream aggregates and dynamic consistency boundary entities.
    /// Registers keyed <see cref="ISnapshotStore"/>, <see cref="IDcbSnapshotStore"/>, and
    /// <see cref="ISnapshotAdmin"/> for this bounded context.
    /// </summary>
    /// <param name="builder">Bounded context already configured with <see cref="UsePostgres"/>.</param>
    /// <param name="configure">
    /// Optional callback for registering per-type snapshot strategies. A type without a
    /// registration keeps <see cref="NeverSnapshotStrategy"/>. For a dynamic consistency
    /// boundary, register the entity type used as <c>TEntity</c> in <c>HandleCommandAsync</c>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining.</returns>
    /// <remarks>
    /// Call <c>PostgresEventStore.InitializeSchemaAsync</c> from a deploy or admin step so
    /// <c>event_snapshots</c> and <c>dcb_snapshots</c> exist. Normal app startup does not
    /// call it. State is stored as bytes. A missing snapshot, a bad checksum,
    /// or a snapshot that cannot be read returns empty and the repository replays the events.
    /// </remarks>
    public static BoundedContextBuilder WithSnapshots(
        this BoundedContextBuilder builder,
        Action<SnapshotStrategyResolver>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var resolver = new SnapshotStrategyResolver();
        configure?.Invoke(resolver);

        var contextName = builder.ContextName;
        builder.Services.AddKeyedSingleton<PostgresSnapshotStore>(contextName, (sp, key) =>
        {
            var options = sp.GetRequiredKeyedService<PostgresEventStoreOptions>((string)key!);
            return new PostgresSnapshotStore(options, sp.GetService<ILogger<PostgresSnapshotStore>>());
        });
        builder.Services.AddKeyedSingleton<ISnapshotStore>(contextName,
            (sp, key) => sp.GetRequiredKeyedService<PostgresSnapshotStore>((string)key!));
        builder.Services.AddKeyedSingleton<IDcbSnapshotStore>(contextName,
            (sp, key) => sp.GetRequiredKeyedService<PostgresSnapshotStore>((string)key!));
        builder.Services.AddKeyedSingleton<ISnapshotAdmin>(contextName,
            (sp, key) => sp.GetRequiredKeyedService<PostgresSnapshotStore>((string)key!));
        builder.Services.AddSingleton<ISnapshotStrategyResolver>(resolver);
        BoundedContextBuilderExtensions.EnsureSnapshotWriteInfrastructure(builder.Services);

        if (string.Equals(contextName, LawnDart.LawnDartNames.DefaultContextName, StringComparison.Ordinal))
        {
            builder.Services.TryAddSingleton<ISnapshotStore>(sp =>
                sp.GetRequiredKeyedService<ISnapshotStore>("default"));
            builder.Services.TryAddSingleton<IDcbSnapshotStore>(sp =>
                sp.GetRequiredKeyedService<IDcbSnapshotStore>("default"));
            builder.Services.TryAddSingleton<ISnapshotAdmin>(sp =>
                sp.GetRequiredKeyedService<ISnapshotAdmin>("default"));
        }

        return builder;
    }
}

file sealed class CompositeOutboxPublisher : IOutboxPublisher
{
    private readonly IOutboxPublisher[] _publishers;

    public CompositeOutboxPublisher(IOutboxPublisher[] publishers) => _publishers = publishers;

    /// <summary>Publishes one outbox row through each registered publisher.</summary>
    public async Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        foreach (var publisher in _publishers)
            await publisher.PublishAsync(message, cancellationToken).ConfigureAwait(false);
    }
}

file sealed class PostgresBoundedContextEventStore : IBoundedContextEventStore
{
    public PostgresBoundedContextEventStore(string contextName, IEventStore eventStore)
    {
        ContextName = contextName;
        EventStore = eventStore;
    }

    /// <summary>Bounded context name.</summary>
    public string ContextName { get; }

    /// <summary>Store registered for <see cref="ContextName"/>.</summary>
    public IEventStore EventStore { get; }
}
