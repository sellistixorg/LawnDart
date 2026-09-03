using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.Context;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using Microsoft.Extensions.Options;
using LawnDart.Aggregates;
using LawnDart.Authorization;
using LawnDart.Dcb;
using LawnDart.Messaging;
using LawnDart.Metadata;
using LawnDart.Snapshots;
using LawnDart.Tagging;

namespace LawnDart.EventSourcing;

/// <summary>
/// Extension methods on <see cref="BoundedContextBuilder"/> provided by the
/// <c>LawnDart.EventSourcing</c> package.
/// </summary>
public static class BoundedContextBuilderExtensions
{
    // -------------------------------------------------------------------------
    // UseInMemory
    // -------------------------------------------------------------------------

    /// <summary>
    /// Configures this bounded context to use the in-memory event store.
    /// </summary>
    /// <remarks>
    /// All context-specific services (<see cref="IEventStore"/>,
    /// <see cref="IAggregateRepository"/>, <see cref="IDcbRepository"/>) are registered
    /// as keyed singletons/scoped services using <see cref="BoundedContextBuilder.ContextName"/>
    /// as the key. The conventional <c>"default"</c> context also gets unkeyed aliases
    /// (same instances, <c>TryAdd</c>) so single-context HTTP handlers can inject
    /// <see cref="IEventStore"/> without a hand-written bridge. Named contexts stay keyed-only.
    /// </remarks>
    /// <returns>The builder for further chaining.</returns>
    public static BoundedContextBuilder UseInMemory(this BoundedContextBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var services     = builder.Services;
        var contextName  = builder.ContextName;

        // Keyed IEventStore + IStreamRegistry + portable subscriptions
        services.AddKeyedSingleton<IEventStore>(contextName,
            (_, _) => new InMemoryEventStore(contextName: contextName));
        services.AddKeyedSingleton<IStreamRegistry>(contextName,
            (sp, key) => (IStreamRegistry)sp.GetRequiredKeyedService<IEventStore>(key!));
        services.AddKeyedSingleton<IEventStoreSubscriptions>(contextName,
            (sp, key) => (IEventStoreSubscriptions)sp.GetRequiredKeyedService<IEventStore>(key!));

        // Keyed repositories — transient (scoped not supported for keyed in all scenarios)
        services.AddKeyedTransient<IAggregateRepository>(contextName,
            (sp, key) => new AggregateRepository(
                sp.GetRequiredKeyedService<IEventStore>(key!),
                sp.GetRequiredService<IMetadataProvider>(),
                sp.GetRequiredService<ITenantContextProvider>(),
                sp.GetRequiredService<IOptions<LawnDartOptions>>(),
                sp.GetKeyedService<ITagProvider>(key!) ?? sp.GetService<ITagProvider>(),
                sp.GetService<AuthorizationService>(),
                sp.GetService<Microsoft.Extensions.Logging.ILogger<AggregateRepository>>(),
                sp.GetKeyedService<ISnapshotStore>(key!) ?? sp.GetService<ISnapshotStore>(),
                sp.GetService<ISnapshotStrategyResolver>()));

        services.AddKeyedTransient<IDcbRepository>(contextName,
            (sp, key) => new DcbRepository(
                sp.GetRequiredKeyedService<IEventStore>(key!),
                sp.GetRequiredService<IMetadataProvider>(),
                sp.GetRequiredService<ITenantContextProvider>(),
                sp.GetRequiredService<IOptions<LawnDartOptions>>(),
                sp.GetKeyedService<ITagProvider>(key!) ?? sp.GetService<ITagProvider>(),
                sp.GetService<AuthorizationService>(),
                sp.GetService<Microsoft.Extensions.Logging.ILogger<DcbRepository>>(),
                sp.GetService<IOptions<EventSourcingOptions>>(),
                sp.GetKeyedService<IDcbSnapshotStore>(key!) ?? sp.GetService<IDcbSnapshotStore>(),
                sp.GetService<ISnapshotStrategyResolver>()));

        TryAddDefaultUnkeyedAliases(services, contextName);

        // Register IBoundedContextEventStore for discovery / diagnostics
        services.AddSingleton<IBoundedContextEventStore>(sp =>
            new DefaultBoundedContextEventStore(
                contextName,
                sp.GetRequiredKeyedService<IEventStore>(contextName)));

        EnsureSharedInfrastructure(services);

        return builder;
    }

    // -------------------------------------------------------------------------
    // WithTagProvider
    // -------------------------------------------------------------------------

    /// <summary>
    /// Registers a context-specific <see cref="ITagProvider"/> for this bounded context.
    /// </summary>
    /// <typeparam name="T">The tag provider implementation type.</typeparam>
    /// <param name="builder">The bounded context builder.</param>
    /// <returns>The same builder for fluent chaining.</returns>
    /// <remarks>
    /// <para>
    /// The provider is registered as a keyed singleton using the context name as the key.
    /// Repositories belonging to this context will prefer the keyed provider over any
    /// global (non-keyed) <see cref="ITagProvider"/> registered via
    /// <see cref="LawnDartExtensions.AddTagProvider{T}(Microsoft.Extensions.DependencyInjection.IServiceCollection)"/>.
    /// </para>
    /// <para>
    /// Use this overload when different bounded contexts require different tagging strategies.
    /// For a single-context application a global registration with
    /// <c>services.AddTagProvider&lt;T&gt;()</c> is simpler.
    /// </para>
    /// </remarks>
    public static BoundedContextBuilder WithTagProvider<T>(this BoundedContextBuilder builder)
        where T : class, ITagProvider
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddKeyedSingleton<ITagProvider, T>(builder.ContextName);
        return builder;
    }

    /// <summary>
    /// Registers a context-specific <see cref="ITagProvider"/> instance for this bounded context.
    /// </summary>
    /// <param name="builder">The bounded context builder.</param>
    /// <param name="instance">The tag provider instance to use.</param>
    /// <returns>The same builder for fluent chaining.</returns>
    /// <remarks>
    /// Use when the tag provider requires constructor arguments or must be shared across
    /// multiple bounded contexts with the same instance.
    /// </remarks>
    public static BoundedContextBuilder WithTagProvider(
        this BoundedContextBuilder builder,
        ITagProvider instance)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(instance);
        builder.Services.AddKeyedSingleton<ITagProvider>(builder.ContextName, instance);
        return builder;
    }

    /// <summary>
    /// Registers a context-specific <see cref="ITagProvider"/> using a factory for this bounded context.
    /// </summary>
    /// <param name="builder">The bounded context builder.</param>
    /// <param name="factory">Factory delegate that receives the <see cref="IServiceProvider"/> and returns the provider.</param>
    /// <returns>The same builder for fluent chaining.</returns>
    public static BoundedContextBuilder WithTagProvider(
        this BoundedContextBuilder builder,
        Func<IServiceProvider, ITagProvider> factory)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(factory);
        builder.Services.AddKeyedSingleton<ITagProvider>(
            builder.ContextName,
            (sp, _) => factory(sp));
        return builder;
    }

    // -------------------------------------------------------------------------
    // WithCommandHandlers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Scans the supplied assemblies for <see cref="ICommandHandler{TCommand}"/>
    /// implementations, registers them as keyed services for this context, and
    /// populates the <see cref="ICommandContextRegistry"/> so that the
    /// <see cref="ContextAwareCommandDispatcher"/> can route commands automatically.
    /// </summary>
    /// <param name="builder">The bounded context builder.</param>
    /// <param name="assemblies">
    /// One or more assemblies to scan.  When empty, the calling assembly is used.
    /// Each assembly should contain handlers for <em>only this context</em>; scanning
    /// the same assembly across multiple contexts causes duplicate-registration errors.
    /// For multi-context test scenarios use
    /// <see cref="WithCommandHandlers(BoundedContextBuilder, Type[])"/> to specify
    /// handler types explicitly.
    /// </param>
    /// <returns>The builder for further chaining.</returns>
    public static BoundedContextBuilder WithCommandHandlers(
        this BoundedContextBuilder builder,
        params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (assemblies.Length == 0)
            assemblies = [Assembly.GetCallingAssembly()];

        var services    = builder.Services;
        var contextName = builder.ContextName;
        var registry    = GetOrCreateCommandRegistry(services);

        var handlerInterface = typeof(ICommandHandler<>);

        foreach (var assembly in assemblies)
        {
            foreach (var type in assembly.GetTypes())
            {
                if (!type.IsClass || type.IsAbstract || type.IsGenericTypeDefinition)
                    continue;

                foreach (var iface in type.GetInterfaces())
                {
                    if (!iface.IsGenericType) continue;
                    if (iface.GetGenericTypeDefinition() != handlerInterface) continue;

                    var commandType = iface.GetGenericArguments()[0];
                    RegisterHandlerCore(services, contextName, registry, type, commandType);
                }
            }
        }

        EnsureSharedInfrastructure(services);

        return builder;
    }

    /// <summary>
    /// Registers the specified handler types explicitly for this bounded context.
    /// </summary>
    /// <remarks>
    /// Use this overload in multi-context test scenarios where multiple contexts share the
    /// same assembly and you need to assign specific handler types to each context without
    /// scanning the entire assembly.
    /// </remarks>
    /// <param name="builder">The bounded context builder.</param>
    /// <param name="handlerTypes">Concrete handler types to register.</param>
    /// <returns>The builder for further chaining.</returns>
    public static BoundedContextBuilder WithCommandHandlers(
        this BoundedContextBuilder builder,
        Type[] handlerTypes)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handlerTypes);

        var services     = builder.Services;
        var contextName  = builder.ContextName;
        var registry     = GetOrCreateCommandRegistry(services);
        var handlerIface = typeof(ICommandHandler<>);

        foreach (var type in handlerTypes)
        {
            if (!type.IsClass || type.IsAbstract)
                throw new ArgumentException($"Handler type '{type.FullName}' must be a concrete class.");

            foreach (var iface in type.GetInterfaces())
            {
                if (!iface.IsGenericType) continue;
                if (iface.GetGenericTypeDefinition() != handlerIface) continue;

                var commandType = iface.GetGenericArguments()[0];
                RegisterHandlerCore(services, contextName, registry, type, commandType);
            }
        }

        EnsureSharedInfrastructure(services);

        return builder;
    }

    private static void RegisterHandlerCore(
        IServiceCollection services,
        string contextName,
        DefaultCommandContextRegistry registry,
        Type handlerImplType,
        Type commandType)
    {
        // Register in the command-to-context map (enforces uniqueness)
        registry.Register(commandType, contextName);

        // Register the handler as a keyed transient, using ContextServiceProvider
        // so the handler's injected dependencies are resolved from the correct context.
        var closedHandlerInterface = typeof(ICommandHandler<>).MakeGenericType(commandType);

        services.AddKeyedTransient(closedHandlerInterface, contextName,
            (sp, _) => ActivatorUtilities.CreateInstance(
                new ContextServiceProvider(sp, contextName),
                handlerImplType));
    }

    // -------------------------------------------------------------------------
    // Shared infrastructure helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// For <c>"default"</c> only: unkeyed aliases that forward to the keyed registrations.
    /// Named contexts stay keyed-only so two stores cannot collide on one unkeyed
    /// <see cref="IEventStore"/>. Uses <c>TryAdd</c> so a prior alias (same instance) is kept.
    /// </summary>
    private static void TryAddDefaultUnkeyedAliases(IServiceCollection services, string contextName)
    {
        if (!string.Equals(contextName, "default", StringComparison.Ordinal))
            return;

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


    /// <summary>
    /// Ensures that the shared infrastructure (registry, dispatcher, validator) required
    /// by the bounded context system is registered exactly once.
    /// </summary>
    private static void EnsureSharedInfrastructure(IServiceCollection services)
    {
        // ICommandContextRegistry — singleton shared by all contexts
        services.TryAddSingleton<ICommandContextRegistry, DefaultCommandContextRegistry>();

        // ContextAwareCommandDispatcher — registered as ICommandDispatcher
        services.TryAddSingleton<ICommandDispatcher, ContextAwareCommandDispatcher>();

        // Startup validator
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, ContextStartupValidator>());
    }

    private static DefaultCommandContextRegistry GetOrCreateCommandRegistry(IServiceCollection services)
    {
        for (var i = 0; i < services.Count; i++)
        {
            if (services[i].ServiceType == typeof(ICommandContextRegistry) &&
                services[i].ImplementationInstance is DefaultCommandContextRegistry existing)
            {
                return existing;
            }
        }

        // Not yet registered — create a concrete instance and register it as both
        // the concrete type and the interface so WithCommandHandlers can find it again.
        var registry = new DefaultCommandContextRegistry();
        services.AddSingleton<ICommandContextRegistry>(registry);
        return registry;
    }
}

/// <summary>
/// Simple record-style implementation of <see cref="IBoundedContextEventStore"/> used when
/// the in-memory backend is configured via <c>UseInMemory()</c>.
/// </summary>
file sealed class DefaultBoundedContextEventStore : IBoundedContextEventStore
{
    public string ContextName { get; }
    public IEventStore EventStore { get; }

    internal DefaultBoundedContextEventStore(string contextName, IEventStore eventStore)
    {
        ContextName = contextName;
        EventStore  = eventStore;
    }
}
