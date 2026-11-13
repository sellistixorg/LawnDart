using System.Reflection;
using System.Runtime.CompilerServices;
using LawnDart.Publishing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace LawnDart.EventStore;

/// <summary>
/// Fluent builder for registering all services that belong to a single bounded context.
/// </summary>
/// <remarks>
/// <para>
/// Obtain an instance via <see cref="BoundedContextExtensions.AddBoundedContext"/>.
/// Backend packages extend this class with <c>UseInMemory()</c> and
/// <c>UseSqlServer()</c>. Additional backends extend it the same way.
/// </para>
/// <para>
/// Every keyed service registration targets <see cref="ContextName"/> as the DI key, so a
/// single application can host multiple event stores for different contexts without conflicts.
/// </para>
/// </remarks>
public sealed class BoundedContextBuilder
{
    /// <summary>The DI key for all services registered by this builder.</summary>
    public string ContextName { get; }

    /// <summary>The service collection that builder methods should register into.</summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Scoped catalog registered by <c>WithEventTypes</c>, or <c>null</c> when
    /// the context has not been warmed up yet.
    /// </summary>
    public EventTypeCatalog? EventCatalog { get; internal set; }

    internal EventUpcastPipeline? UpcastPipeline { get; set; }

    /// <summary>Map registered by <c>PublishEvents</c>, or <c>null</c> when nothing is declared.</summary>
    internal PublishedEventMap? PublishedEvents { get; set; }

    internal BoundedContextBuilder(string contextName, IServiceCollection services)
    {
        ContextName = contextName;
        Services    = services;
    }
}

/// <summary>Extension methods that attach a <see cref="BoundedContextBuilder"/> to an <see cref="IServiceCollection"/>.</summary>
public static class BoundedContextExtensions
{
    /// <summary>
    /// Registers a new bounded context and returns a builder for configuring its services.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="contextName">
    /// The unique name for this bounded context (e.g. <c>"ordering"</c>, <c>"catalog"</c>,
    /// or <c>"default"</c>).  Used as the keyed-DI key for all context-specific services.
    /// </param>
    /// <returns>A <see cref="BoundedContextBuilder"/> for further configuration.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="contextName"/> is null, whitespace, a case variant of
    /// <see cref="LawnDartNames.DefaultContextName"/>, or (when
    /// <see cref="LawnDartOptions.ValidateNames"/> is true) outside <see cref="LawnDartNames.Pattern"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">Thrown when the same context name is registered more than once. Comparison ignores case.</exception>
    public static BoundedContextBuilder AddBoundedContext(
        this IServiceCollection services,
        string contextName)
    {
        LawnDartNames.RequireContextName(services, contextName, nameof(contextName));

        // Guard: prevent duplicate context names (case-insensitive)
        var registry = GetOrCreateRegistry(services);
        registry.Register(contextName);

        return new BoundedContextBuilder(contextName, services);
    }

    /// <summary>
    /// Scans <typeparamref name="TMarker"/>'s assembly for concrete
    /// <see cref="IEvent"/> types and registers them as this context's catalog.
    /// Prefer this overload — it is refactor-proof.
    /// </summary>
    public static BoundedContextBuilder WithEventTypes<TMarker>(
        this BoundedContextBuilder builder)
        => WithEventTypes(builder, typeof(TMarker).Assembly);

    /// <summary>
    /// Invokes the empty-assembly scan so tests can assert the
    /// <c>GetCallingAssembly</c> fallback message.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static BoundedContextBuilder WithEventTypesUsingCallingAssembly(
        this BoundedContextBuilder builder)
        => WithEventTypes(builder, Array.Empty<Assembly>());

    /// <summary>
    /// Scans assemblies for concrete <see cref="IEvent"/> types and registers them
    /// as this context's event-type catalog. Types must declare
    /// <see cref="EventTypeNameAttribute"/>. Duplicate <c>(token, version)</c>,
    /// two current types, a multi-type event name with no <c>current: true</c>,
    /// abstract types, and non-events make <see cref="EventTypeCatalog.Materialize"/> throw.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The scan discovered no concrete <see cref="IEvent"/> types.
    /// </exception>
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static BoundedContextBuilder WithEventTypes(
        this BoundedContextBuilder builder,
        params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var usedCallingAssemblyFallback = assemblies.Length == 0;
        if (usedCallingAssemblyFallback)
            assemblies = [Assembly.GetCallingAssembly()];

        var types = new List<Type>();
        foreach (var assembly in assemblies)
        {
            ArgumentNullException.ThrowIfNull(assembly);
            foreach (var type in assembly.GetExportedTypes())
            {
                if (!type.IsClass || type.IsAbstract || type.IsGenericTypeDefinition)
                    continue;
                if (!typeof(IEvent).IsAssignableFrom(type) || typeof(IRawEvent).IsAssignableFrom(type))
                    continue;
                types.Add(type);
            }
        }

        if (types.Count == 0)
        {
            throw new InvalidOperationException(
                FormatZeroEventScanMessage(assemblies, usedCallingAssemblyFallback));
        }

        RegisterCatalog(builder, types);
        return builder;
    }

    /// <summary>
    /// Registers the given event types as this context's catalog.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="types"/> is empty.
    /// </exception>
    public static BoundedContextBuilder WithEventTypes(
        this BoundedContextBuilder builder,
        params Type[] types)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(types);
        if (types.Length == 0)
            throw new InvalidOperationException("WithEventTypes was given no event types.");
        RegisterCatalog(builder, types);
        return builder;
    }

    /// <summary>
    /// Scans <typeparamref name="TMarker"/>'s assembly for concrete
    /// <see cref="IEventUpcaster{TTo, TFrom}"/> types and builds this
    /// context's upcast pipeline. Prefer this overload — it is refactor-proof.
    /// Call <see cref="WithEventTypes(BoundedContextBuilder, Type[])"/> first.
    /// </summary>
    public static BoundedContextBuilder WithUpcasters<TMarker>(
        this BoundedContextBuilder builder)
        => WithUpcasters(builder, typeof(TMarker).Assembly);

    /// <summary>
    /// Scans assemblies for concrete <see cref="IEventUpcaster{TTo, TFrom}"/>
    /// types and builds this context's upcast pipeline. Every historical
    /// catalog version must reach current. There is no downcast API and no
    /// AppDomain sweep. Call <see cref="WithEventTypes(BoundedContextBuilder, Assembly[])"/> first.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static BoundedContextBuilder WithUpcasters(
        this BoundedContextBuilder builder,
        params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (assemblies.Length == 0)
            assemblies = [Assembly.GetCallingAssembly()];

        var types = new List<Type>();
        foreach (var assembly in assemblies)
        {
            ArgumentNullException.ThrowIfNull(assembly);
            foreach (var type in assembly.GetExportedTypes())
            {
                if (!type.IsClass || type.IsAbstract || type.IsGenericTypeDefinition)
                    continue;
                if (!ImplementsUpcaster(type))
                    continue;
                types.Add(type);
            }
        }

        RegisterPipeline(builder, types);
        return builder;
    }

    /// <summary>
    /// Registers the given upcaster types as this context's pipeline.
    /// Call <see cref="WithEventTypes(BoundedContextBuilder, Type[])"/> first.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="types"/> is empty.
    /// </exception>
    public static BoundedContextBuilder WithUpcasters(
        this BoundedContextBuilder builder,
        params Type[] types)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(types);
        if (types.Length == 0)
            throw new InvalidOperationException("WithUpcasters was given no upcaster types.");
        RegisterPipeline(builder, types);
        return builder;
    }

    /// <summary>
    /// Declares the events this context publishes through the outbox: each one passes
    /// through as is (<see cref="PublishedEventsBuilder.Publish{TEvent}"/>) or is turned
    /// into an integration event (<see cref="PublishedEventsBuilder.Translate{TInternal, TPublic}(Func{TInternal, TPublic})"/>).
    /// An event that is not declared is never published.
    /// Call <see cref="WithEventTypes(BoundedContextBuilder, Assembly[])"/> first.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Registers a <see cref="PublishedEventMap"/> keyed by <see cref="BoundedContextBuilder.ContextName"/>
    /// and one process-wide <see cref="PublishedEventCatalog"/> holding every context's published types.
    /// </para>
    /// <para>
    /// Fails at startup when nothing is declared (the message lists this context's event
    /// types), a published type has no <see cref="EventTypeNameAttribute"/>, a declared
    /// internal event is not in this context's catalog, one internal event is declared
    /// twice, another context already publishes the same event name with a different
    /// .NET type, or this method was already called for this context.
    /// </para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">A startup check fails.</exception>
    public static BoundedContextBuilder PublishEvents(
        this BoundedContextBuilder builder,
        Action<PublishedEventsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var catalog = builder.EventCatalog
            ?? throw new InvalidOperationException(
                "PublishEvents requires WithEventTypes first on this bounded context.");
        if (builder.PublishedEvents is not null)
        {
            throw new InvalidOperationException(
                $"PublishEvents has already been called for context '{builder.ContextName}'.");
        }

        var map = PublishedEventMap.Create(builder.ContextName, catalog, configure);
        var registry = GetOrCreatePublishedRegistry(builder.Services);
        registry.Add(map);

        builder.PublishedEvents = map;
        builder.Services.AddKeyedSingleton(builder.ContextName, map);
        if (string.Equals(builder.ContextName, LawnDartNames.DefaultContextName, StringComparison.Ordinal))
            builder.Services.TryAddSingleton(map);
        return builder;
    }

    private static void RegisterCatalog(BoundedContextBuilder builder, IReadOnlyCollection<Type> types)
    {
        var catalog = EventTypeCatalog.Materialize(types);
        builder.EventCatalog = catalog;
        builder.Services.AddKeyedSingleton<IEventTypeCatalog>(builder.ContextName, catalog);
        if (string.Equals(builder.ContextName, LawnDartNames.DefaultContextName, StringComparison.Ordinal))
            builder.Services.TryAddSingleton<IEventTypeCatalog>(catalog);
    }

    private static void RegisterPipeline(BoundedContextBuilder builder, IReadOnlyCollection<Type> types)
    {
        var catalog = builder.EventCatalog
            ?? throw new InvalidOperationException(
                "WithUpcasters requires WithEventTypes first on this bounded context.");
        if (builder.UpcastPipeline is not null)
        {
            throw new InvalidOperationException(
                $"WithUpcasters has already been called for context '{builder.ContextName}'.");
        }

        var pipeline = EventUpcastPipeline.Materialize(catalog, types);
        builder.UpcastPipeline = pipeline;
        builder.Services.AddKeyedSingleton(builder.ContextName, pipeline);
        if (string.Equals(builder.ContextName, LawnDartNames.DefaultContextName, StringComparison.Ordinal))
            builder.Services.TryAddSingleton(pipeline);
    }

    private static bool ImplementsUpcaster(Type type)
    {
        foreach (var iface in type.GetInterfaces())
        {
            if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IEventUpcaster<,>))
                return true;
        }

        return false;
    }

    private static string FormatZeroEventScanMessage(
        IReadOnlyList<Assembly> assemblies,
        bool usedCallingAssemblyFallback)
    {
        var names = string.Join(", ", assemblies.Select(a => $"'{a.GetName().Name}'"));
        var scanned = assemblies.Count == 1
            ? $"assembly {names}"
            : $"assemblies {names}";

        if (usedCallingAssemblyFallback)
        {
            return $"WithEventTypes scanned {scanned} (chosen by Assembly.GetCallingAssembly() because no assembly was passed) and found no IEvent implementations.";
        }

        return $"WithEventTypes scanned {scanned} and found no IEvent implementations.";
    }

    /// <summary>
    /// Message when a context has a store but no <c>WithEventTypes</c> catalog.
    /// </summary>
    internal static string MissingEventTypesMessage(string contextName)
        => $"Bounded context '{contextName}' has no event-type catalog. " +
           "Call WithEventTypes(...) when configuring this context. " +
           "The catalog is per-context and required at startup.";

    /// <summary>
    /// Message when a context turns the outbox on and declares nothing to publish.
    /// Lists the context's event types.
    /// </summary>
    internal static string MissingPublishedEventsMessage(string contextName, EventTypeCatalog? catalog)
        => PublishedEventMap.NothingDeclaredMessage(contextName, catalog);

    private static PublishedEventRegistry GetOrCreatePublishedRegistry(IServiceCollection services)
    {
        for (var i = 0; i < services.Count; i++)
        {
            if (services[i].ServiceType == typeof(PublishedEventRegistry) &&
                services[i].ImplementationInstance is PublishedEventRegistry existing)
            {
                return existing;
            }
        }

        var registry = new PublishedEventRegistry();
        services.AddSingleton(registry);
        services.TryAddSingleton(_ => PublishedEventCatalog.Create(registry.Maps));
        return registry;
    }

    private static BoundedContextRegistry GetOrCreateRegistry(IServiceCollection services)
    {
        // Locate an existing BoundedContextRegistry descriptor (registered as itself)
        for (var i = 0; i < services.Count; i++)
        {
            if (services[i].ServiceType == typeof(BoundedContextRegistry) &&
                services[i].ImplementationInstance is BoundedContextRegistry existing)
            {
                return existing;
            }
        }

        var registry = new BoundedContextRegistry();
        services.AddSingleton(registry);
        services.AddSingleton<IBoundedContextRegistry>(registry);
        return registry;
    }
}
