using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
    /// (e.g. from <c>AddServiceBusMessaging</c> or <c>AddInMemoryMessaging</c>).
    /// </summary>
    /// <param name="services">Service collection.</param>
    /// <param name="eventTypes">
    /// Concrete <see cref="IEvent"/> types that may appear in outbox <c>EventType</c> values
    /// (typically the same set registered with the event store).
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
            return new MessageTransportOutboxPublisher(transport, captured);
        });

        return services;
    }
}
