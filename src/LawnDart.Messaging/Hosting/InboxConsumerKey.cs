namespace LawnDart.Messaging.Hosting;

/// <summary>
/// Builds the inbox key hosted services pass to <see cref="LawnDart.Messaging.IInboxStore"/>.
/// One shared store serves every consumer, so the key is the consumer plus the message id.
/// </summary>
internal static class InboxConsumerKey
{
    internal static string ForReactor(Type reactorType, string messageId)
        => Compose("reactor", reactorType, messageId);

    internal static string ForProcessor(Type processorType, string messageId)
        => Compose("processor", processorType, messageId);

    private static string Compose(string role, Type consumerType, string messageId)
    {
        var name = consumerType.FullName ?? consumerType.Name;
        return role + ":" + name + ":" + messageId;
    }
}
