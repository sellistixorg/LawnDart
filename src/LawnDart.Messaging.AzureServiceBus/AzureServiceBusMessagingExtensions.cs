using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.AzureServiceBus;

/// <summary>
/// DI registration for the Azure Service Bus message transport.
/// </summary>
public static class AzureServiceBusMessagingExtensions
{
    /// <summary>
    /// Registers <see cref="AzureServiceBusMessageTransport"/>, <see cref="AzureServiceBusTopologySetup"/>,
    /// and an in-memory <see cref="IDeadLetterQueue"/>.
    /// Calls <see cref="MessagingExtensions.AddMessaging"/> so the host runs
    /// <c>MessageTransportStartupCheck</c>.
    /// A <see cref="ServiceBusClient"/> or <see cref="ServiceBusAdministrationClient"/> already
    /// registered in <paramref name="services"/> is kept.
    /// Reactors and processors still need an <see cref="IInboxStore"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Connection, prefetch, retry, and dead-letter settings.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddAzureServiceBusTransport(
        this IServiceCollection services,
        Action<AzureServiceBusTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddMessaging();
        services.AddOptions<AzureServiceBusTransportOptions>();
        if (!services.Any(d => d.ServiceType == typeof(ApplyCommonMessageTransportOptions)))
        {
            services.AddSingleton<ApplyCommonMessageTransportOptions>();
            services.AddSingleton<IConfigureOptions<AzureServiceBusTransportOptions>>(sp =>
                sp.GetRequiredService<ApplyCommonMessageTransportOptions>());
            services.AddSingleton<IPostConfigureOptions<AzureServiceBusTransportOptions>>(sp =>
                sp.GetRequiredService<ApplyCommonMessageTransportOptions>());
        }

        if (configure is not null)
            services.Configure(configure);

        services.AddSingleton<IPostConfigureOptions<AzureServiceBusTransportOptions>>(sp =>
            new ApplyRegisteredTopicMapper(sp.GetService<ITopicMapper>()));

        foreach (var descriptor in services.Where(d => d.ServiceType == typeof(IOptions<MessageTransportOptions>)).ToList())
            services.Remove(descriptor);

        services.AddSingleton<IOptions<MessageTransportOptions>>(sp =>
            new OptionsWrapper<MessageTransportOptions>(
                sp.GetRequiredService<IOptions<AzureServiceBusTransportOptions>>().Value));

        services.TryAddSingleton<MemoryDeadLetterQueue>();
        services.TryAddSingleton<IDeadLetterQueue>(sp => sp.GetRequiredService<MemoryDeadLetterQueue>());

        services.TryAddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<AzureServiceBusTransportOptions>>().Value;
            if (!options.HasDataConnection)
            {
                throw new InvalidOperationException(
                    "Set AzureServiceBusTransportOptions.ConnectionString, or FullyQualifiedNamespace and Credential, or register a ServiceBusClient.");
            }

            return AzureServiceBusClients.CreateClient(options);
        });
        services.TryAddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<AzureServiceBusTransportOptions>>().Value;
            if (!options.HasAdminConnection)
            {
                throw new InvalidOperationException(
                    "Register a ServiceBusAdministrationClient, or set a connection string or a namespace and credential, so topics and subscriptions can be created.");
            }

            return AzureServiceBusClients.CreateAdmin(options);
        });

        services.TryAddSingleton(sp => new AzureServiceBusConnector(
            sp.GetRequiredService<IOptions<AzureServiceBusTransportOptions>>().Value,
            () => sp.GetRequiredService<ServiceBusClient>(),
            () => sp.GetRequiredService<ServiceBusAdministrationClient>(),
            ownsClient: false,
            ownsAdmin: false));

        services.Replace(ServiceDescriptor.Singleton(sp => new AzureServiceBusMessageTransport(
            sp.GetRequiredService<ILogger<AzureServiceBusMessageTransport>>(),
            sp.GetRequiredService<IOptions<AzureServiceBusTransportOptions>>(),
            sp.GetServices<IMessagePreHandler>(),
            sp.GetRequiredService<IDeadLetterQueue>(),
            sp.GetRequiredService<AzureServiceBusConnector>(),
            ownsConnector: false)));
        services.Replace(ServiceDescriptor.Singleton<IMessageTransport>(sp =>
            sp.GetRequiredService<AzureServiceBusMessageTransport>()));

        services.Replace(ServiceDescriptor.Singleton(sp => new AzureServiceBusTopologySetup(
            sp.GetServices<MessageSubscription>(),
            sp.GetRequiredService<IOptions<AzureServiceBusTransportOptions>>(),
            sp.GetRequiredService<AzureServiceBusConnector>())));
        services.Replace(ServiceDescriptor.Singleton<IMessageTopologySetup>(sp =>
            sp.GetRequiredService<AzureServiceBusTopologySetup>()));

        return services;
    }
}
