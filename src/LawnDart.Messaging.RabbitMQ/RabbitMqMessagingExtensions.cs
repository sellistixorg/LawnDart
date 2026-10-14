using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// DI registration for the RabbitMQ message transport.
/// </summary>
public static class RabbitMqMessagingExtensions
{
    /// <summary>
    /// Registers <see cref="RabbitMqMessageTransport"/>, <see cref="RabbitMqTopologySetup"/>,
    /// and an in-memory <see cref="IDeadLetterQueue"/>.
    /// Calls <see cref="MessagingExtensions.AddMessaging"/> so the host runs
    /// <c>MessageTransportStartupCheck</c>.
    /// Reactors and processors still need an <see cref="IInboxStore"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Connection, prefetch, retry, and dead-letter settings.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRabbitMqTransport(
        this IServiceCollection services,
        Action<RabbitMqTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddMessaging();
        services.AddOptions<RabbitMqTransportOptions>();
        if (configure is not null)
            services.Configure(configure);

        foreach (var descriptor in services.Where(d => d.ServiceType == typeof(IOptions<MessageTransportOptions>)).ToList())
            services.Remove(descriptor);

        services.AddSingleton<IOptions<MessageTransportOptions>>(sp =>
            new OptionsWrapper<MessageTransportOptions>(
                sp.GetRequiredService<IOptions<RabbitMqTransportOptions>>().Value));

        services.TryAddSingleton<MemoryDeadLetterQueue>();
        services.TryAddSingleton<IDeadLetterQueue>(sp => sp.GetRequiredService<MemoryDeadLetterQueue>());
        services.TryAddSingleton(sp =>
            new RabbitMqConnector(sp.GetRequiredService<IOptions<RabbitMqTransportOptions>>().Value));

        services.Replace(ServiceDescriptor.Singleton(sp => new RabbitMqMessageTransport(
            sp.GetRequiredService<ILogger<RabbitMqMessageTransport>>(),
            sp.GetRequiredService<IOptions<RabbitMqTransportOptions>>(),
            sp.GetServices<IMessagePreHandler>(),
            sp.GetRequiredService<IDeadLetterQueue>(),
            sp.GetRequiredService<RabbitMqConnector>())));
        services.Replace(ServiceDescriptor.Singleton<IMessageTransport>(sp =>
            sp.GetRequiredService<RabbitMqMessageTransport>()));

        services.Replace(ServiceDescriptor.Singleton(sp => new RabbitMqTopologySetup(
            sp.GetServices<MessageSubscription>(),
            sp.GetRequiredService<IOptions<RabbitMqTransportOptions>>(),
            sp.GetRequiredService<RabbitMqConnector>())));
        services.Replace(ServiceDescriptor.Singleton<IMessageTopologySetup>(sp =>
            sp.GetRequiredService<RabbitMqTopologySetup>()));

        return services;
    }
}
