using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.AwsSqs;

/// <summary>
/// DI registration for the Amazon SQS message transport.
/// </summary>
public static class AwsSqsMessagingExtensions
{
    /// <summary>
    /// Registers <see cref="AwsSqsMessageTransport"/>, <see cref="AwsSqsTopologySetup"/>,
    /// and an in-memory <see cref="IDeadLetterQueue"/>.
    /// Calls <see cref="MessagingExtensions.AddMessaging"/> so the host runs
    /// <c>MessageTransportStartupCheck</c>.
    /// An <see cref="IAmazonSQS"/> or <see cref="IAmazonSimpleNotificationService"/> already
    /// registered in <paramref name="services"/> is kept.
    /// Reactors and processors still need an <see cref="IInboxStore"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Region, endpoint, prefetch, retry, and dead-letter settings.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddAwsSqsTransport(
        this IServiceCollection services,
        Action<AwsSqsTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddMessaging();
        services.AddOptions<AwsSqsTransportOptions>();
        if (!services.Any(d => d.ServiceType == typeof(ApplyCommonMessageTransportOptions)))
        {
            services.AddSingleton<ApplyCommonMessageTransportOptions>();
            services.AddSingleton<IConfigureOptions<AwsSqsTransportOptions>>(sp =>
                sp.GetRequiredService<ApplyCommonMessageTransportOptions>());
            services.AddSingleton<IPostConfigureOptions<AwsSqsTransportOptions>>(sp =>
                sp.GetRequiredService<ApplyCommonMessageTransportOptions>());
        }

        if (configure is not null)
            services.Configure(configure);

        services.AddSingleton<IPostConfigureOptions<AwsSqsTransportOptions>>(sp =>
            new ApplyRegisteredTopicMapper(sp.GetService<ITopicMapper>()));

        foreach (var descriptor in services.Where(d => d.ServiceType == typeof(IOptions<MessageTransportOptions>)).ToList())
            services.Remove(descriptor);

        services.AddSingleton<IOptions<MessageTransportOptions>>(sp =>
            new OptionsWrapper<MessageTransportOptions>(
                sp.GetRequiredService<IOptions<AwsSqsTransportOptions>>().Value));

        services.TryAddSingleton<MemoryDeadLetterQueue>();
        services.TryAddSingleton<IDeadLetterQueue>(sp => sp.GetRequiredService<MemoryDeadLetterQueue>());

        services.TryAddSingleton<IAmazonSQS>(sp =>
            AwsSqsClients.CreateSqs(sp.GetRequiredService<IOptions<AwsSqsTransportOptions>>().Value));
        services.TryAddSingleton<IAmazonSimpleNotificationService>(sp =>
            AwsSqsClients.CreateSns(sp.GetRequiredService<IOptions<AwsSqsTransportOptions>>().Value));

        services.TryAddSingleton(sp => new AwsSqsConnector(
            sp.GetRequiredService<IOptions<AwsSqsTransportOptions>>().Value,
            () => sp.GetRequiredService<IAmazonSQS>(),
            () => sp.GetRequiredService<IAmazonSimpleNotificationService>(),
            ownsClients: false,
            sp.GetRequiredService<ILogger<AwsSqsConnector>>()));

        services.Replace(ServiceDescriptor.Singleton(sp => new AwsSqsMessageTransport(
            sp.GetRequiredService<ILogger<AwsSqsMessageTransport>>(),
            sp.GetRequiredService<IOptions<AwsSqsTransportOptions>>(),
            sp.GetServices<IMessagePreHandler>(),
            sp.GetRequiredService<IDeadLetterQueue>(),
            sp.GetRequiredService<AwsSqsConnector>(),
            ownsConnector: false,
            sp.GetServices<MessageSubscription>())));
        services.Replace(ServiceDescriptor.Singleton<IMessageTransport>(sp =>
            sp.GetRequiredService<AwsSqsMessageTransport>()));

        services.Replace(ServiceDescriptor.Singleton(sp => new AwsSqsTopologySetup(
            sp.GetServices<MessageSubscription>(),
            sp.GetRequiredService<IOptions<AwsSqsTransportOptions>>(),
            sp.GetRequiredService<AwsSqsConnector>())));
        services.Replace(ServiceDescriptor.Singleton<IMessageTopologySetup>(sp =>
            sp.GetRequiredService<AwsSqsTopologySetup>()));

        return services;
    }
}
