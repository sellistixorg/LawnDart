using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using LawnDart.EventStore;
using LawnDart.Outbox;

namespace LawnDart.Messaging.Outbox;

/// <summary>
/// DI helpers for <see cref="MessageTransportOutboxPublisher"/>.
/// </summary>
public static class MessageTransportOutboxPublisherExtensions
{
    /// <summary>
    /// Registers <see cref="MessageTransportOutboxPublisher"/> as the singleton
    /// <see cref="IOutboxPublisher"/>. Requires an <see cref="IMessageTransport"/>
    /// (e.g. from <c>AddInMemoryMessaging</c>).
    /// </summary>
    /// <param name="services">Service collection.</param>
    /// <param name="eventTypes">
    /// Concrete <see cref="IEvent"/> types that may appear in outbox <c>EventType</c> values
    /// (typically the same set registered with the event store). When omitted, the
    /// publisher uses <see cref="IEventTypeCatalog"/> from DI or
    /// <see cref="EventTypeCatalog.Shared"/>.
    /// </param>
    public static IServiceCollection AddMessageTransportOutboxPublisher(
        this IServiceCollection services,
        params Type[] eventTypes)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(eventTypes);

        var captured = eventTypes.ToArray();
        services.TryAddSingleton<IOutboxPublisher>(sp =>
        {
            var transport = sp.GetRequiredService<IMessageTransport>();
            var catalog = sp.GetService<IEventTypeCatalog>() ?? EventTypeCatalog.Shared;
            if (captured.Length > 0)
                return new MessageTransportOutboxPublisher(transport, captured);
            return new MessageTransportOutboxPublisher(transport, catalog);
        });

        return services;
    }
}
