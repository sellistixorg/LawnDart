using LawnDart.Messaging;
using LawnDart.Metadata;

namespace LawnDart.Demo.Shop.Infrastructure;

/// <summary>
/// Host choices that handlers need without taking a dependency on the store type.
/// </summary>
public sealed class ShopRuntimeOptions
{
    /// <summary>
    /// When true, handlers publish reactor events on <see cref="IMessageTransport"/>.
    /// When false, the SQL outbox publishes those events after commit.
    /// </summary>
    public bool PublishDirectly { get; init; } = true;
}

/// <summary>
/// Publishes a domain event onto the in-process transport with the caller's tenant and correlation.
/// </summary>
public static class ShopMessages
{
    /// <summary>
    /// Publishes <paramref name="evt"/> when <paramref name="publishDirectly"/> is true.
    /// </summary>
    public static Task PublishAsync<TEvent>(
        IMessageTransport transport,
        TEvent evt,
        CommandMetadata meta,
        bool publishDirectly,
        CancellationToken cancellationToken)
        where TEvent : class, IEvent
    {
        if (!publishDirectly)
            return Task.CompletedTask;

        var scope = CorrelationScope.Current;
        var context = new MessageContext
        {
            MessageId = Guid.NewGuid().ToString(),
            CorrelationId = scope?.CorrelationId ?? meta.CorrelationId,
            CausationId = meta.CausationId,
            UserId = meta.UserId ?? scope?.UserId,
            TenantId = meta.TenantId
        };

        return transport.PublishAsync(evt, context, cancellationToken);
    }
}
