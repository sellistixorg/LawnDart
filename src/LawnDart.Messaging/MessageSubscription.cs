namespace LawnDart.Messaging;

/// <summary>
/// A message type registered with the host.
/// <c>AddReactor</c> and <c>AddEventProcessor</c> add one entry per call.
/// </summary>
public sealed class MessageSubscription
{
    /// <summary>
    /// Records <paramref name="messageType"/> for startup checks and topology setup.
    /// </summary>
    /// <param name="messageType">CLR message type. It must declare <c>[EventTypeName]</c>.</param>
    public MessageSubscription(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        MessageType = messageType;
    }

    /// <summary>CLR message type for this subscription.</summary>
    public Type MessageType { get; }
}
