namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// Exchange, queue, and routing names for one declared message type.
/// </summary>
public static class RabbitMqNames
{
    /// <summary>
    /// Direct exchange for <paramref name="messageType"/>.
    /// The value is the declared type name from <see cref="MessageTypeNames.Require"/>.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    public static string Exchange(string messageType) => messageType;

    /// <summary>
    /// Routing key for <paramref name="messageType"/>.
    /// The value is the declared type name.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    public static string RoutingKey(string messageType) => messageType;

    /// <summary>
    /// Queue that receives <paramref name="messageType"/> for one subscription.
    /// The value is <c>{messageType}.{subscriptionName}</c>.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    public static string Queue(string messageType, string subscriptionName)
        => messageType + "." + subscriptionName;

    /// <summary>
    /// Dead-letter exchange for <paramref name="messageType"/>.
    /// The value is <c>{messageType}.dlx</c>.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    public static string DeadLetterExchange(string messageType) => messageType + ".dlx";

    /// <summary>
    /// Routing key on the dead-letter exchange for one subscription.
    /// The value is <c>{messageType}.{subscriptionName}</c>, so one subscription's
    /// dead-letter queue does not receive another subscription's messages.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    public static string DeadLetterRoutingKey(string messageType, string subscriptionName)
        => messageType + "." + subscriptionName;

    /// <summary>
    /// Dead-letter queue for <paramref name="messageType"/> and one subscription.
    /// The value is <c>{messageType}.{subscriptionName}.dlq</c>.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    public static string DeadLetterQueue(string messageType, string subscriptionName)
        => messageType + "." + subscriptionName + ".dlq";
}
