namespace LawnDart.Messaging;

/// <summary>
/// Internal marker for a message type with an in-process subscriber registered
/// through <c>AddReactor</c> or <c>AddEventProcessor</c>.
/// </summary>
internal sealed class InProcessSubscription
{
    public InProcessSubscription(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        MessageType = messageType;
    }

    public Type MessageType { get; }
}
