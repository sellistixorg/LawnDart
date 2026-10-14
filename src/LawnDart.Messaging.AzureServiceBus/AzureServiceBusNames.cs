namespace LawnDart.Messaging.AzureServiceBus;

/// <summary>
/// Topic and subscription names for one declared message type.
/// </summary>
public static class AzureServiceBusNames
{
    /// <summary>Maximum length of a Service Bus topic name.</summary>
    public const int TopicNameMaxLength = 260;

    /// <summary>Maximum length of a Service Bus subscription name.</summary>
    public const int SubscriptionNameMaxLength = 50;

    /// <summary>
    /// Topic for <paramref name="messageType"/>.
    /// The value is the declared type name from <see cref="MessageTypeNames.Require"/>.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    public static string Topic(string messageType)
    {
        RequireTopic(messageType);
        return messageType;
    }

    /// <summary>
    /// Subscription that receives <paramref name="subscriptionName"/> on a topic.
    /// One subscriber uses one subscription name on every topic.
    /// </summary>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    public static string Subscription(string subscriptionName)
    {
        RequireSubscription(subscriptionName);
        return subscriptionName;
    }

    /// <summary>
    /// Throws when <paramref name="messageType"/> is not a Service Bus topic name.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    public static void RequireTopic(string messageType)
        => RequireEntity(messageType, "topic", TopicNameMaxLength);

    /// <summary>
    /// Throws when <paramref name="subscriptionName"/> is not a Service Bus subscription name.
    /// </summary>
    /// <param name="subscriptionName">Consumer name.</param>
    public static void RequireSubscription(string subscriptionName)
        => RequireEntity(subscriptionName, "subscription", SubscriptionNameMaxLength);

    private static void RequireEntity(string name, string kind, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(name) || name.Length > maxLength || !IsEntityName(name))
        {
            throw new InvalidOperationException(
                $"Azure Service Bus {kind} name '{name}' is invalid. " +
                $"Use 1 to {maxLength} letters, digits, periods, hyphens, or underscores, and start and end with a letter or digit.");
        }
    }

    private static bool IsEntityName(string name)
    {
        if (!char.IsLetterOrDigit(name[0]) || !char.IsLetterOrDigit(name[^1]))
            return false;

        foreach (var character in name)
        {
            if (char.IsLetterOrDigit(character) || character is '.' or '-' or '_')
                continue;
            return false;
        }

        return true;
    }
}
