using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Serialization;

namespace LawnDart;

/// <summary>
/// Extension methods for registering core LawnDart services.
/// </summary>
public static class LawnDartExtensions
{
    /// <summary>
    /// Adds core LawnDart services to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional callback for <see cref="LawnDartOptions"/>.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// Registers default metadata generation using <see cref="DefaultMetadataProvider"/>,
    /// <see cref="Tagging.LowercaseTagFormat"/> as <see cref="Tagging.ITagFormat"/>
    /// (kept when one is already registered), and a no-tenant
    /// <see cref="AmbientTenantContextProvider"/> so
    /// <c>IAggregateRepository</c> resolves without a hand-written tenant line.
    /// Replace the provider with <see cref="AddTenantContextProvider{T}"/> when the
    /// host is multi-tenant. Replace the format with
    /// <see cref="AddTagFormat{T}(IServiceCollection)"/>.
    /// </remarks>
    public static IServiceCollection AddLawnDart(
        this IServiceCollection services,
        Action<LawnDartOptions>? configure = null)
    {
        // Register options
        if (configure != null)
        {
            services.Configure(configure);
        }
        else
        {
            services.Configure<LawnDartOptions>(_ => { });
        }

        services.TryAddSingleton<ITenantContextProvider, AmbientTenantContextProvider>();
        services.TryAddSingleton<Tagging.ITagFormat, Tagging.LowercaseTagFormat>();
        services.TryAddSingleton(_ => new Tagging.UnknownTagWarningLog());

        // Register default metadata provider if not already registered
        services.AddSingleton<IMetadataProvider>(sp =>
        {
            var tenantProvider = sp.GetService<ITenantContextProvider>();
            var catalog = sp.GetService<IEventTypeCatalog>();
            return new DefaultMetadataProvider(tenantProvider, catalog);
        });

        return services;
    }

    /// <summary>
    /// Adds a tenant context provider to the service collection.
    /// </summary>
    /// <typeparam name="T">The tenant context provider type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// The provider is registered as a singleton implementation of
    /// <see cref="ITenantContextProvider"/>.
    /// </remarks>
    public static IServiceCollection AddTenantContextProvider<T>(this IServiceCollection services)
        where T : class, ITenantContextProvider
    {
        services.AddSingleton<ITenantContextProvider, T>();
        return services;
    }

    /// <summary>
    /// Adds a metadata provider to the service collection.
    /// </summary>
    /// <typeparam name="T">The metadata provider type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// Use this to replace the default <see cref="IMetadataProvider"/> with a custom
    /// implementation.
    /// </remarks>
    public static IServiceCollection AddMetadataProvider<T>(this IServiceCollection services)
        where T : class, IMetadataProvider
    {
        services.AddSingleton<IMetadataProvider, T>();
        return services;
    }

    /// <summary>
    /// Adds a global tag provider to the service collection, shared by all bounded contexts.
    /// </summary>
    /// <typeparam name="T">The tag provider implementation type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// <para>
    /// The provider is registered as a non-keyed singleton and is used by every bounded
    /// context that does not have its own context-specific tag provider.
    /// </para>
    /// <para>
    /// For single-context applications or when one tagging strategy covers the whole app this
    /// is the simplest registration path.  For multi-context applications where different
    /// contexts need different strategies, use
    /// <see cref="AddTagProvider{T}(IServiceCollection,string)"/> or the fluent
    /// <c>.WithTagProvider&lt;T&gt;()</c> method on the <c>BoundedContextBuilder</c>.
    /// </para>
    /// </remarks>
    public static IServiceCollection AddTagProvider<T>(this IServiceCollection services)
        where T : class, Tagging.ITagProvider
    {
        services.AddSingleton<Tagging.ITagProvider, T>();
        return services;
    }

    /// <summary>
    /// Adds a context-specific tag provider that applies only to the named bounded context.
    /// </summary>
    /// <typeparam name="T">The tag provider implementation type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="contextName">
    /// The bounded context name (DI key) this provider should be scoped to.
    /// Must match the name passed to <c>AddBoundedContext(contextName)</c>.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// <para>
    /// The provider is registered as a keyed singleton.  Repositories belonging to the
    /// named context will prefer this keyed provider over any global (non-keyed) provider.
    /// Contexts without their own keyed provider automatically fall back to the global one.
    /// </para>
    /// <para>
    /// Prefer the fluent API when building the context inline:
    /// <code>
    /// services.AddBoundedContext("shop")
    ///     .UseInMemory()
    ///     .WithTagProvider&lt;ShopTagProvider&gt;();
    /// </code>
    /// Use this overload when configuring the context and its tag provider in separate
    /// places (e.g. in different extension methods or feature modules).
    /// </para>
    /// </remarks>
    public static IServiceCollection AddTagProvider<T>(
        this IServiceCollection services,
        string contextName)
        where T : class, Tagging.ITagProvider
    {
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", nameof(contextName));

        services.AddKeyedSingleton<Tagging.ITagProvider, T>(contextName);
        return services;
    }

    /// <summary>
    /// Adds a global tag format shared by every bounded context that has no keyed format.
    /// </summary>
    /// <typeparam name="T">The tag format implementation type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// <para>
    /// <see cref="AddLawnDart"/> registers <see cref="Tagging.LowercaseTagFormat"/> with
    /// <c>TryAdd</c>. Call this before <see cref="AddLawnDart"/> to keep <typeparamref name="T"/>,
    /// or after it to replace the default. The last registration of <see cref="Tagging.ITagFormat"/>
    /// is the one that resolves.
    /// </para>
    /// <para>
    /// For one context, use <see cref="AddTagFormat{T}(IServiceCollection, string)"/> or
    /// <c>WithTagFormat&lt;T&gt;()</c> on the bounded context builder.
    /// </para>
    /// </remarks>
    public static IServiceCollection AddTagFormat<T>(this IServiceCollection services)
        where T : class, Tagging.ITagFormat
    {
        services.AddSingleton<Tagging.ITagFormat, T>();
        return services;
    }

    /// <summary>
    /// Adds a tag format that applies only to the named bounded context.
    /// </summary>
    /// <typeparam name="T">The tag format implementation type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="contextName">
    /// The bounded context name (dependency injection key). Must match
    /// <c>AddBoundedContext(contextName)</c>.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// Repositories for <paramref name="contextName"/> prefer this keyed format.
    /// A context with no keyed format uses the global <see cref="Tagging.ITagFormat"/>.
    /// </remarks>
    public static IServiceCollection AddTagFormat<T>(
        this IServiceCollection services,
        string contextName)
        where T : class, Tagging.ITagFormat
    {
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", nameof(contextName));

        services.AddKeyedSingleton<Tagging.ITagFormat, T>(contextName);
        return services;
    }

}
