using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using LawnDart.Authorization;

namespace LawnDart.Authorization.AspNetCore;

/// <summary>
/// DI extensions for HTTP authorization context integration.
/// </summary>
public static class AuthorizationAspNetCoreExtensions
{
    /// <summary>
    /// Registers <see cref="HttpAuthorizationContextProvider"/> as the authorization context provider
    /// for ASP.NET Core hosts (claims / headers from <c>HttpContext</c>).
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddHttpAuthorizationContext(this IServiceCollection services)
    {
        services.AddHttpContextAccessor();
        services.TryAddSingleton<HttpAuthorizationContextProvider>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<AuthorizationContextProviderRegistration, HttpAuthorizationContextProviderRegistration>());
        services.TryAddSingleton<IAuthorizationContextProvider>(
            sp => sp.GetRequiredService<HttpAuthorizationContextProvider>());
        return services;
    }

    /// <summary>
    /// Distinct registration type so TryAddEnumerable can distinguish this leaf from other transports.
    /// </summary>
    private sealed class HttpAuthorizationContextProviderRegistration : AuthorizationContextProviderRegistration
    {
        public HttpAuthorizationContextProviderRegistration(HttpAuthorizationContextProvider provider)
            : base(provider)
        {
        }
    }
}
