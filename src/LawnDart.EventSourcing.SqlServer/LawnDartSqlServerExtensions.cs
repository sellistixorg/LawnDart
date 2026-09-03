using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart.Aggregates;
using LawnDart.Authorization;
using LawnDart.Dcb;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventSourcing.SqlServer.Outbox;
using LawnDart.EventSourcing.SqlServer.Snapshots;
using LawnDart.EventSourcing.Outbox;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Outbox;
using LawnDart.Serialization;
using LawnDart.EventSourcing.Serialization;
using LawnDart.Snapshots;
using LawnDart.Tagging;
using Microsoft.Extensions.Logging;

namespace LawnDart.EventSourcing.SqlServer;

/// <summary>
/// Extension methods for registering SQL Server event sourcing services.
/// </summary>
public static class LawnDartSqlServerExtensions
{
    // -------------------------------------------------------------------------
    // BoundedContextBuilder fluent API — UseSqlServer
    // -------------------------------------------------------------------------

    /// <summary>
    /// Configures this bounded context to use the SQL Server event store.
    /// Tables and sequences are created in the schema specified by
    /// <see cref="SqlServerEventStoreOptions.SchemaName"/> (defaults to <c>"dbo"</c>).
    /// </summary>
    /// <remarks>
    /// All context-specific services (<see cref="IEventStore"/>,
    /// <see cref="IStreamRegistry"/>, <see cref="IAggregateRepository"/>,
    /// <see cref="IDcbRepository"/>) are registered as keyed singletons/transients
    /// using <see cref="BoundedContextBuilder.ContextName"/> as the DI key.
    /// The conventional <c>"default"</c> context also gets unkeyed aliases (same instances,
    /// <c>TryAdd</c>) so single-context HTTP handlers can inject those types without a
    /// hand-written bridge. Named contexts stay keyed-only.
    /// </remarks>
    /// <param name="builder">The bounded-context builder.</param>
    /// <param name="configure">
    /// Callback that must provide at least <see cref="SqlServerEventStoreOptions.ConnectionString"/>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining (<c>WithSnapshots</c>, <c>WithCommandHandlers</c>).</returns>
    public static BoundedContextBuilder UseSqlServer(
        this BoundedContextBuilder builder,
        Action<SqlServerEventStoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var services    = builder.Services;
        var contextName = builder.ContextName;

        // Build options eagerly so we can read SchemaName / ConnectionString at registration time.
        var options = new SqlServerEventStoreOptions { ContextName = contextName };
        configure(options);

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            throw new ArgumentException(
                $"ConnectionString must be set in the SqlServerEventStoreOptions configure action " +
                $"for bounded context '{contextName}'.");

        // Ensure a JSON serializer is available
        services.TryAddSingleton<IEventSerializer, JsonEventSerializer>();

        // Keyed outbox writer (created regardless; only used when EnableOutbox = true)
        services.AddKeyedSingleton<IOutboxWriter>(contextName, (sp, _) =>
            new SqlServerOutboxWriter(
                options.ConnectionString,
                options.OutboxTableName,
                sp.GetService<ILogger<SqlServerOutboxWriter>>(),
                options.SchemaName));

        // Keyed event store
        services.AddKeyedSingleton<IEventStore>(contextName, (sp, key) =>
        {
            var serializer  = sp.GetRequiredService<IEventSerializer>();
            var outbox      = options.EnableOutbox
                ? sp.GetRequiredKeyedService<IOutboxWriter>(key!)
                : null;
            var logger      = sp.GetService<ILogger<SqlServerEventStore>>();
            return new SqlServerEventStore(
                options.ConnectionString,
                serializer,
                options.EventStoreTableName,
                options.StreamRegistryTableName,
                options.EnableStreamRegistry && options.AutoMaintainRegistry,
                options,
                outbox,
                logger);
        });

        // Keyed stream registry (SqlServerEventStore implements IStreamRegistry)
        services.AddKeyedSingleton<IStreamRegistry>(contextName,
            (sp, key) => (IStreamRegistry)sp.GetRequiredKeyedService<IEventStore>(key!));

        // Portable subscriptions (same store instance)
        services.AddKeyedSingleton<IEventStoreSubscriptions>(contextName,
            (sp, key) => (IEventStoreSubscriptions)sp.GetRequiredKeyedService<IEventStore>(key!));

        // Keyed repositories
        services.AddKeyedTransient<IAggregateRepository>(contextName, (sp, key) =>
            new AggregateRepository(
                sp.GetRequiredKeyedService<IEventStore>(key!),
                sp.GetRequiredService<IMetadataProvider>(),
                sp.GetRequiredService<ITenantContextProvider>(),
                sp.GetRequiredService<IOptions<LawnDartOptions>>(),
                sp.GetKeyedService<ITagProvider>(key!) ?? sp.GetService<ITagProvider>(),
                sp.GetService<AuthorizationService>(),
                sp.GetService<ILogger<AggregateRepository>>(),
                sp.GetKeyedService<ISnapshotStore>(key!) ?? sp.GetService<ISnapshotStore>(),
                sp.GetService<ISnapshotStrategyResolver>()));

        services.AddKeyedTransient<IDcbRepository>(contextName, (sp, key) =>
            new DcbRepository(
                sp.GetRequiredKeyedService<IEventStore>(key!),
                sp.GetRequiredService<IMetadataProvider>(),
                sp.GetRequiredService<ITenantContextProvider>(),
                sp.GetRequiredService<IOptions<LawnDartOptions>>(),
                sp.GetKeyedService<ITagProvider>(key!) ?? sp.GetService<ITagProvider>(),
                sp.GetService<AuthorizationService>(),
                sp.GetService<ILogger<DcbRepository>>(),
                sp.GetService<IOptions<EventSourcingOptions>>(),
                sp.GetKeyedService<IDcbSnapshotStore>(key!) ?? sp.GetService<IDcbSnapshotStore>(),
                sp.GetService<ISnapshotStrategyResolver>()));

        if (string.Equals(contextName, "default", StringComparison.Ordinal))
        {
            services.TryAddSingleton<IEventStore>(sp =>
                sp.GetRequiredKeyedService<IEventStore>("default"));
            services.TryAddSingleton<IStreamRegistry>(sp =>
                sp.GetRequiredKeyedService<IStreamRegistry>("default"));
            services.TryAddSingleton<IEventStoreSubscriptions>(sp =>
                sp.GetRequiredKeyedService<IEventStoreSubscriptions>("default"));
            services.TryAddTransient<IAggregateRepository>(sp =>
                sp.GetRequiredKeyedService<IAggregateRepository>("default"));
            services.TryAddTransient<IDcbRepository>(sp =>
                sp.GetRequiredKeyedService<IDcbRepository>("default"));
        }

        // Register IBoundedContextEventStore for discovery / diagnostics
        services.AddSingleton<IBoundedContextEventStore>(sp =>
            new SqlServerBoundedContextEventStore(
                contextName,
                sp.GetRequiredKeyedService<IEventStore>(contextName)));

        // Conditionally register outbox processor as hosted service for this context
        if (options.EnableOutbox)
        {
            services.AddSingleton<IHostedService>(sp =>
            {
                var outboxWriter = sp.GetRequiredKeyedService<IOutboxWriter>(contextName);
                var publishers   = sp.GetServices<IOutboxPublisher>().ToArray();

                if (publishers.Length == 0)
                    throw new InvalidOperationException(
                        $"EnableOutbox is true for context '{contextName}' but no IOutboxPublisher is registered.");

                IOutboxPublisher publisher = publishers.Length == 1
                    ? publishers[0]
                    : new CompositeOutboxPublisher(publishers);

                return new OutboxProcessor(
                    outboxWriter,
                    publisher,
                    sp.GetService<ILogger<OutboxProcessor>>() ?? NullLogger<OutboxProcessor>.Instance);
            });
        }

        services.AddKeyedSingleton(contextName, options);

        return builder;
    }

    /// <summary>
    /// Enables SQL Server snapshot storage for stream aggregates and DCB entities.
    /// Registers keyed <see cref="ISnapshotStore"/>, <see cref="IDcbSnapshotStore"/>, and
    /// <see cref="ISnapshotAdmin"/> for this bounded context.
    /// </summary>
    /// <param name="builder">Bounded context already configured with <see cref="UseSqlServer"/>.</param>
    /// <param name="configure">
    /// Optional callback for registering per-type snapshot strategies. Types without a
    /// registration keep <see cref="NeverSnapshotStrategy"/>. For DCB, register the
    /// entity type used as <c>TEntity</c> in <c>HandleCommandAsync</c>, not the
    /// <c>IState</c> class.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining.</returns>
    /// <remarks>
    /// This method does not run DDL. Call <c>SqlServerEventStore.InitializeSchemaAsync</c>
    /// so <c>DcbSnapshots</c> and <c>EventSnapshots</c> exist in the context
    /// <see cref="SqlServerEventStoreOptions.SchemaName"/>.
    /// </remarks>
    public static BoundedContextBuilder WithSnapshots(
        this BoundedContextBuilder builder,
        Action<SnapshotStrategyResolver>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var resolver = new SnapshotStrategyResolver();
        configure?.Invoke(resolver);

        var contextName = builder.ContextName;

        builder.Services.AddKeyedSingleton<SqlServerSnapshotStore>(contextName, (sp, key) =>
        {
            var options = sp.GetRequiredKeyedService<SqlServerEventStoreOptions>((string)key!);
            return new SqlServerSnapshotStore(
                options,
                options.EventStoreTableName,
                sp.GetService<ILogger<SqlServerSnapshotStore>>());
        });

        builder.Services.AddKeyedSingleton<ISnapshotStore>(contextName,
            (sp, key) => sp.GetRequiredKeyedService<SqlServerSnapshotStore>((string)key!));
        builder.Services.AddKeyedSingleton<IDcbSnapshotStore>(contextName,
            (sp, key) => sp.GetRequiredKeyedService<SqlServerSnapshotStore>((string)key!));
        builder.Services.AddKeyedSingleton<ISnapshotAdmin>(contextName,
            (sp, key) => sp.GetRequiredKeyedService<SqlServerSnapshotStore>((string)key!));
        builder.Services.AddSingleton<ISnapshotStrategyResolver>(resolver);

        if (string.Equals(contextName, "default", StringComparison.Ordinal))
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

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private class CompositeOutboxPublisher : IOutboxPublisher
    {
        private readonly IOutboxPublisher[] _publishers;

        public CompositeOutboxPublisher(IOutboxPublisher[] publishers) =>
            _publishers = publishers;

        public async Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            foreach (var p in _publishers)
                await p.PublishAsync(message, cancellationToken);
        }
    }
}

/// <summary>
/// Minimal <see cref="IBoundedContextEventStore"/> wrapper for a SQL Server–backed context.
/// </summary>
file sealed class SqlServerBoundedContextEventStore : IBoundedContextEventStore
{
    public string     ContextName { get; }
    public IEventStore EventStore  { get; }

    internal SqlServerBoundedContextEventStore(string contextName, IEventStore eventStore)
    {
        ContextName = contextName;
        EventStore  = eventStore;
    }
}
