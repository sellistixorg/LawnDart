namespace LawnDart.Messaging;

/// <summary>
/// Extension methods for <see cref="IMessageTransport"/> that enable runtime-type dispatch.
/// </summary>
public static class MessageTransportExtensions
{
    private static readonly System.Reflection.MethodInfo PublishAsyncMethod =
        typeof(IMessageTransport).GetMethod(nameof(IMessageTransport.PublishAsync))!;

    /// <summary>
    /// Publishes an <see cref="IEvent"/> using its <em>runtime</em> concrete type as
    /// <c>TMessage</c>, so that subscribers registered under the concrete type receive it.
    /// </summary>
    public static Task PublishEventAsync(
        this IMessageTransport transport,
        IEvent @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        var method = PublishAsyncMethod.MakeGenericMethod(@event.GetType());
        return (Task)method.Invoke(transport, [@event, context, cancellationToken])!;
    }
}
