using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Outbox;
using LawnDart.Publishing;

namespace LawnDart.Messaging.Outbox;

/// <summary>
/// DI helpers for <see cref="MessageTransportOutboxPublisher"/>.
/// </summary>
public static class MessageTransportOutboxPublisherExtensions
{
    /// <summary>
    /// Registers <see cref="MessageTransportOutboxPublisher"/> as the singleton
    /// <see cref="IOutboxPublisher"/>. Requires an <see cref="IMessageTransport"/>
    /// from <c>AddInMemoryMessaging</c>, <c>AddRabbitMqTransport</c>,
    /// <c>AddAzureServiceBusTransport</c>, or <c>AddAwsSqsTransport</c>.
    /// </summary>
    /// <param name="services">Service collection.</param>
    /// <param name="eventTypes">
    /// Concrete <see cref="IEvent"/> types that may appear in outbox <c>EventType</c> values.
    /// When omitted, the publisher reads rows through <see cref="PublishedEventCatalog"/>,
    /// the union of every context's published types. When omitted and that catalog
    /// is missing, resolution fails.
    /// </param>
    public static IServiceCollection AddMessageTransportOutboxPublisher(
        this IServiceCollection services,
        params Type[] eventTypes)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(eventTypes);

        var captured = eventTypes.ToArray();
        EnsureListenerCheck(services, captured);
        services.TryAddSingleton<IOutboxPublisher>(sp =>
        {
            var transport = sp.GetRequiredService<IMessageTransport>();
            if (captured.Length > 0)
                return new MessageTransportOutboxPublisher(transport, captured);

            var catalog = sp.GetService<PublishedEventCatalog>()
                ?? throw new InvalidOperationException(
                    "AddMessageTransportOutboxPublisher requires PublishEvents on a bounded context, " +
                    "or pass the published event types to this method.");
            return new MessageTransportOutboxPublisher(transport, catalog, catalog.UpcastPipeline);
        });

        return services;
    }

    private static void EnsureListenerCheck(IServiceCollection services, Type[] explicitTypes)
    {
        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType == typeof(OutboundListenerPlan) &&
                descriptor.ImplementationInstance is OutboundListenerPlan existing)
            {
                existing.Add(explicitTypes);
                return;
            }
        }

        var plan = new OutboundListenerPlan();
        plan.Add(explicitTypes);
        services.AddSingleton(plan);
        services.AddSingleton<IHostedService>(sp => new OutboundListenerCheck(
            plan,
            sp.GetRequiredService<IMessageTransport>(),
            sp.GetRequiredService<IOptions<MessageTransportOptions>>(),
            sp,
            sp.GetRequiredService<ILogger<OutboundListenerCheck>>()));
    }
}
