using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

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
    /// <exception cref="ArgumentException">Thrown when <paramref name="contextName"/> is null or whitespace.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the same context name is registered more than once.</exception>
    public static BoundedContextBuilder AddBoundedContext(
        this IServiceCollection services,
        string contextName)
    {
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", nameof(contextName));

        // Guard: prevent duplicate context names
        var registry = GetOrCreateRegistry(services);
        registry.Register(contextName);

        return new BoundedContextBuilder(contextName, services);
    }

    /// <summary>
    /// Scans assemblies for concrete <see cref="IEvent"/> types and registers them
    /// as this context's event-type catalog. Types must declare
    /// <see cref="EventTypeNameAttribute"/>. Duplicate tokens, abstract types, and
    /// non-events fail closed.
    /// </summary>
    public static BoundedContextBuilder WithEventTypes(
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
                if (!typeof(IEvent).IsAssignableFrom(type) || typeof(IRawEvent).IsAssignableFrom(type))
                    continue;
                types.Add(type);
            }
        }

        EventTypeNameResolver.Warmup(types);
        return builder;
    }

    /// <summary>
    /// Registers the given event types as this context's catalog.
    /// </summary>
    public static BoundedContextBuilder WithEventTypes(
        this BoundedContextBuilder builder,
        params Type[] types)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(types);
        EventTypeNameResolver.Warmup(types);
        return builder;
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
