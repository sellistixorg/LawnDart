using Microsoft.Extensions.DependencyInjection;

namespace LawnDart.Metadata;

/// <summary>
/// Fluent builder for composing a <see cref="CompositeTenantContextProvider"/>.
/// </summary>
/// <remarks>
/// Obtain an instance via
/// <see cref="TenantContextProviderExtensions.AddCompositeTenantContextProvider"/>.
/// External packages (e.g. <c>LawnDart.AspNetCore</c>) can extend this builder
/// with additional <c>Add…</c> methods without creating circular dependencies.
/// </remarks>
public sealed class CompositeTenantContextProviderBuilder
{
    private readonly List<ITenantContextProvider> _providers = [];

    internal CompositeTenantContextProviderBuilder() { }

    /// <summary>Appends an already-constructed provider instance to the chain.</summary>
    public CompositeTenantContextProviderBuilder Add(ITenantContextProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        _providers.Add(provider);
        return this;
    }

    internal CompositeTenantContextProvider Build()
        => new(_providers.AsReadOnly());
}

/// <summary>
/// <see cref="IServiceCollection"/> extension methods for registering tenant context providers.
/// </summary>
public static class TenantContextProviderExtensions
{
    /// <summary>
    /// Registers a <see cref="CompositeTenantContextProvider"/> as the
    /// <see cref="ITenantContextProvider"/> for this service collection.
    /// </summary>
    /// <remarks>
    /// Use the builder to add providers in priority order (highest priority first).
    /// JWT and HTTP-aware providers must be added via extension methods from their respective
    /// packages (<c>LawnDart.AspNetCore</c>, <c>LawnDart.Projections.Lightweight</c>)
    /// to avoid circular package dependencies.
    /// </remarks>
    /// <example>
    /// <code>
    /// services.AddCompositeTenantContextProvider(b => b
    ///     .Add(new AmbientTenantContextProvider())
    ///     .Add(new FixedTenantContextProvider("shop")));
    /// </code>
    /// </example>
    public static IServiceCollection AddCompositeTenantContextProvider(
        this IServiceCollection services,
        Action<CompositeTenantContextProviderBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new CompositeTenantContextProviderBuilder();
        configure(builder);
        services.AddSingleton<ITenantContextProvider>(builder.Build());
        return services;
    }
}
