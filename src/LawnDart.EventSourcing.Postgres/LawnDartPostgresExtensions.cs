using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.Serialization;
using LawnDart.EventStore;
using LawnDart.Serialization;
using LawnDart.Tagging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace LawnDart.EventSourcing.Postgres;

/// <summary>
/// Registers the Postgres event store on a bounded context.
/// </summary>
public static class LawnDartPostgresExtensions
{
    /// <summary>
    /// Configures this bounded context to use the Postgres event store.
    /// Tables live in <see cref="PostgresEventStoreOptions.SchemaName"/> (default <c>lawndart</c>), never in <c>public</c>.
    /// </summary>
    /// <remarks>
    /// Keyed services use <see cref="BoundedContextBuilder.ContextName"/>.
    /// The context named <c>default</c> also gets unkeyed aliases.
    /// The callback must set <see cref="PostgresEventStoreOptions.ConnectionString"/>.
    /// <see cref="PostgresEventStoreOptions.EnableOutbox"/> must stay false until the outbox release.
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
            ValidateNames = LawnDart.LawnDartNames.IsFormatCheckEnabled(services)
        };
        configure(options);
        options.EnsureValid();

        services.AddKeyedSingleton(contextName, LawnDart.EventSourcing.Context.EventStoreContextMarker.Instance);
        services.TryAddSingleton<IEventSerializer, JsonEventSerializer>();

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
