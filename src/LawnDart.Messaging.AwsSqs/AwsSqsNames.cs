namespace LawnDart.Messaging.AwsSqs;

/// <summary>
/// Topic and queue names for Amazon SNS and SQS.
/// FIFO topics and queues are not supported.
/// </summary>
public static class AwsSqsNames
{
    /// <summary>Maximum length of an SNS topic name.</summary>
    public const int TopicNameMaxLength = 256;

    /// <summary>Maximum length of an SQS queue name.</summary>
    public const int QueueNameMaxLength = 80;

    /// <summary>Separator between the subscription and the topic in a queue name. The value is <c>--</c>.</summary>
    public const string NameSeparator = "--";

    /// <summary>Suffix on the dead-letter queue name. The value is <c>--dlq</c>.</summary>
    public const string DeadLetterSuffix = "--dlq";

    /// <summary>
    /// Message attribute that carries the declared event type.
    /// Publish sets it. An SNS filter matches it when several types share a topic.
    /// The value is <c>lawndart_type</c>.
    /// </summary>
    public const string EventTypeAttribute = "lawndart_type";

    /// <summary>
    /// Validates <paramref name="topic"/> as an SNS topic name and returns it.
    /// Pass the name from <see cref="ITopicMapper.TopicFor"/>.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    public static string Topic(string topic)
    {
        RequireTopic(topic);
        return topic;
    }

    /// <summary>
    /// Queue that receives <paramref name="subscriptionName"/> on <paramref name="topic"/>.
    /// The name is <c>{subscription}--{topic}</c>.
    /// The dead-letter queue appends <c>--dlq</c> and must also fit in <see cref="QueueNameMaxLength"/>.
    /// A name that does not fit is rejected. There is no shortened or hashed form.
    /// </summary>
    /// <param name="topic">Topic name from <see cref="ITopicMapper.TopicFor"/>.</param>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    public static string Queue(string topic, string subscriptionName)
    {
        RequireTopic(topic);
        RequireSubscription(subscriptionName);
        var queue = subscriptionName + NameSeparator + topic;
        RequireQueue(queue);
        var deadLetter = queue + DeadLetterSuffix;
        if (!IsQueueToken(deadLetter))
        {
            throw new InvalidOperationException(
                $"SQS dead-letter queue name '{deadLetter}' is invalid. " +
                $"The queue name is '{queue}', and '{DeadLetterSuffix}' is appended. " +
                $"Both names must be 1 to {QueueNameMaxLength} characters. Shorten the topic or the subscription name.");
        }

        return queue;
    }

    /// <summary>
    /// Dead-letter queue for <paramref name="subscriptionName"/> on <paramref name="topic"/>.
    /// The name is <c>{subscription}--{topic}--dlq</c>.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="subscriptionName">Consumer name.</param>
    public static string DeadLetterQueue(string topic, string subscriptionName)
        => Queue(topic, subscriptionName) + DeadLetterSuffix;

    /// <summary>
    /// Throws when <paramref name="topic"/> is not an SNS topic name.
    /// A name that ends with <c>.fifo</c> is rejected. FIFO is not supported.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    public static void RequireTopic(string topic)
        => RequireToken(topic, "SNS topic", TopicNameMaxLength, fifoKind: "topics");

    /// <summary>
    /// Throws when <paramref name="subscriptionName"/> is not a safe queue-name part.
    /// </summary>
    /// <param name="subscriptionName">Consumer name.</param>
    public static void RequireSubscription(string subscriptionName)
        => RequireToken(subscriptionName, "SQS subscription", QueueNameMaxLength, fifoKind: "queues");

    /// <summary>
    /// Throws when <paramref name="queueName"/> is not an SQS queue name.
    /// </summary>
    /// <param name="queueName">Queue name.</param>
    public static void RequireQueue(string queueName)
        => RequireToken(queueName, "SQS queue", QueueNameMaxLength, fifoKind: "queues");

    /// <summary>
    /// Whole seconds SQS will delay a message. SQS delay is 0 to 900 seconds.
    /// A positive delay below one second becomes 1. <paramref name="retryDelay"/> must already be in range.
    /// </summary>
    /// <param name="retryDelay">Delay from <see cref="AwsSqsTransportOptions.RetryDelay"/>.</param>
    public static int DelaySeconds(TimeSpan retryDelay)
    {
        var seconds = (int)Math.Ceiling(retryDelay.TotalSeconds);
        if (seconds < 1)
            return 1;
        if (seconds > AwsSqsTransportOptions.MaxDelaySeconds)
            return AwsSqsTransportOptions.MaxDelaySeconds;
        return seconds;
    }

    private static void RequireToken(string name, string kind, int maxLength, string fifoKind)
    {
        if (IsFifo(name))
        {
            throw new InvalidOperationException(
                $"SQS FIFO {fifoKind} are not supported. '{name}' ends with .fifo.");
        }

        if (string.IsNullOrWhiteSpace(name) || !IsToken(name, maxLength))
        {
            throw new InvalidOperationException(
                $"{kind} name '{name}' is invalid. " +
                $"Use 1 to {maxLength} letters, digits, hyphens, or underscores. " +
                "A period is not allowed. Map a declared name that contains one, such as order.placed, to a topic that fits these rules.");
        }
    }

    private static bool IsQueueToken(string name) => IsToken(name, QueueNameMaxLength);

    private static bool IsToken(string name, int maxLength)
    {
        if (name.Length is < 1 || name.Length > maxLength)
            return false;

        foreach (var character in name)
        {
            if (char.IsAsciiLetterOrDigit(character) || character is '-' or '_')
                continue;
            return false;
        }

        return true;
    }

    private static bool IsFifo(string name)
        => name.EndsWith(".fifo", StringComparison.Ordinal);
}
