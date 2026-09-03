using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace LawnDart.Messaging.InMemory;

/// <summary>
/// DI registration helpers for the in-memory message transport.
/// </summary>
public static class InMemoryMessagingExtensions
{
    /// <summary>
    /// Registers the in-memory transport and inbox store.
    /// Call this instead of (or before) a production transport registration when running tests or
    /// local development.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional messaging options configurator.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddInMemoryMessaging(
        this IServiceCollection services,
        Action<MessagingOptions>? configure = null)
    {
        services.AddMessaging(configure);

        services.TryAddSingleton<InMemoryMessageTransport>();
        services.TryAddSingleton<IMessageTransport>(sp => sp.GetRequiredService<InMemoryMessageTransport>());

        services.TryAddSingleton<InMemoryInboxStore>();
        services.TryAddSingleton<IInboxStore>(sp => sp.GetRequiredService<InMemoryInboxStore>());

        return services;
    }
}
