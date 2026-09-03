using Microsoft.Extensions.DependencyInjection;
using LawnDart.Authorization;

namespace LawnDart;

/// <summary>
/// Extension methods for registering authorization services.
/// </summary>
public static class LawnDartAuthorizationExtensions
{
    /// <summary>
    /// Adds LawnDart authorization services to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddLawnDartAuthorization(
        this IServiceCollection services,
        Action<AuthorizationOptions>? configure = null)
    {
        services.AddSingleton<IAuthorizationProvider, DefaultAuthorizationProvider>();
        services.AddSingleton<AuthorizationService>();

        if (configure != null)
        {
            services.Configure(configure);
        }

        return services;
    }

    /// <summary>
    /// Adds a composite authorization context provider that tries leaf providers in registration order.
    /// </summary>
    /// <remarks>
    /// Call after satellite <c>AddHttpAuthorizationContext</c> / <c>AddServiceBusAuthorizationContext</c> /
    /// <c>AddBackgroundServiceAuthorizationContext</c>. Leaf providers are discovered via
    /// <see cref="AuthorizationContextProviderRegistration"/> (not via
    /// <c>GetServices&lt;IAuthorizationContextProvider&gt;()</c>, which would recurse into this composite).
    /// This registration becomes the <see cref="IAuthorizationContextProvider"/> resolved by
    /// <see cref="AuthorizationService"/>.
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddCompositeAuthorizationContext(this IServiceCollection services)
    {
        services.AddSingleton<IAuthorizationContextProvider>(sp =>
        {
            var leaves = sp.GetServices<AuthorizationContextProviderRegistration>()
                .Select(r => r.Provider)
                .ToArray();

            if (leaves.Length == 0)
            {
                throw new InvalidOperationException(
                    "AddCompositeAuthorizationContext requires at least one leaf provider. " +
                    "Call AddHttpAuthorizationContext, AddServiceBusAuthorizationContext, and/or " +
                    "AddBackgroundServiceAuthorizationContext (from the Authorization.* packages) first.");
            }

            return new CompositeAuthorizationContextProvider(leaves);
        });

        return services;
    }
}

/// <summary>
/// Options for configuring authorization.
/// </summary>
public class AuthorizationOptions
{
    /// <summary>
    /// Gets or sets whether to throw exceptions on authorization failures.
    /// Default: true
    /// </summary>
    public bool ThrowOnFailure { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to log authorization checks.
    /// Default: true
    /// </summary>
    public bool EnableLogging { get; set; } = true;
}
