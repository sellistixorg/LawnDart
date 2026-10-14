using System.Text;

namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// Exchange, queue, and routing names for one declared message type.
/// </summary>
public static class RabbitMqNames
{
    /// <summary>Maximum length of a RabbitMQ exchange, queue, or routing key, in bytes.</summary>
    public const int NameMaxLength = 255;

    /// <summary>
    /// Direct exchange for <paramref name="topic"/>.
    /// Pass the name from <see cref="ITopicMapper.TopicFor"/>.
    /// The default mapper returns the declared type name.
    /// </summary>
    /// <param name="topic">Topic or exchange name.</param>
    public static string Exchange(string topic) => topic;

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

    /// <summary>
    /// Exchange that returns a delayed message to one subscription queue.
    /// The value is <c>{messageType}.{subscriptionName}.delay</c>.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    public static string DelayExchange(string messageType, string subscriptionName)
        => messageType + "." + subscriptionName + ".delay";

    /// <summary>
    /// Queue that holds a delivery for <see cref="RabbitMqTransportOptions.RetryDelay"/>
    /// and then dead-letters it back to the subscription queue.
    /// The value is <c>{messageType}.{subscriptionName}.retry</c>.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    public static string RetryQueue(string messageType, string subscriptionName)
        => messageType + "." + subscriptionName + ".retry";

    /// <summary>Routing key from the delay exchange back to the subscription queue. The value is <c>return</c>.</summary>
    public const string DelayRoutingKey = "return";

    /// <summary>
    /// Throws when <paramref name="exchange"/> is not a RabbitMQ exchange name.
    /// </summary>
    /// <param name="exchange">Exchange name from <see cref="ITopicMapper.TopicFor"/>.</param>
    public static void RequireExchange(string exchange)
        => Require(exchange, "exchange");

    /// <summary>
    /// Throws when <paramref name="routingKey"/> is not a RabbitMQ routing key.
    /// </summary>
    /// <param name="routingKey">Routing key. This transport uses the declared message type name.</param>
    public static void RequireRoutingKey(string routingKey)
        => Require(routingKey, "routing key");

    /// <summary>
    /// Throws when <paramref name="queue"/> is not a RabbitMQ queue name.
    /// </summary>
    /// <param name="queue">Queue name.</param>
    public static void RequireQueue(string queue)
        => Require(queue, "queue");

    private static void Require(string name, string kind)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new InvalidOperationException(
                $"RabbitMQ {kind} name is required.");
        }

        if (Encoding.UTF8.GetByteCount(name) > NameMaxLength)
        {
            throw new InvalidOperationException(
                $"RabbitMQ {kind} name '{name}' is longer than {NameMaxLength} bytes.");
        }

        if (name.StartsWith("amq.", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"RabbitMQ {kind} name '{name}' is reserved. Names that start with 'amq.' are not allowed.");
        }
    }
}
