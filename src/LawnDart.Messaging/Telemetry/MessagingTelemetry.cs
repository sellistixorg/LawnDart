using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;

namespace LawnDart.Messaging.Telemetry;

/// <summary>
/// OpenTelemetry instrumentation for the message transport layer.
/// </summary>
public static class MessagingTelemetry
{
    /// <summary>Meter name for transport counters and histograms. Value is <c>LawnDart.Messaging</c>.</summary>
    public const string MeterName = "LawnDart.Messaging";

    /// <summary>Activity source name for publish and consume spans. Value is <c>LawnDart.Messaging</c>.</summary>
    public const string ActivitySourceName = "LawnDart.Messaging";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);
    private static readonly Meter Meter = new(MeterName);

    private static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
        "messaging.publish.duration", "ms", "Time taken to publish a message to the transport");

    private static readonly Histogram<double> ConsumeDuration = Meter.CreateHistogram<double>(
        "messaging.consume.duration", "ms", "Time taken to consume and dispatch a message");

    private static readonly Counter<long> PublishErrors = Meter.CreateCounter<long>(
        "messaging.publish.errors", "count", "Number of publish failures");

    private static readonly Counter<long> InboxDuplicates = Meter.CreateCounter<long>(
        "messaging.inbox.duplicates", "count", "Messages discarded due to inbox deduplication");

    private static readonly Counter<long> DeliveryRetries = Meter.CreateCounter<long>(
        "messaging.delivery.retries", "count", "Failed delivery attempts that will be tried again");

    private static readonly Counter<long> DeadLettered = Meter.CreateCounter<long>(
        "messaging.deadlettered", "count", "Messages parked after they used their delivery attempts");

    /// <summary>Starts a publish activity span.</summary>
    public static Activity? StartPublish(string messageType)
    {
        var activity = ActivitySource.StartActivity("Messaging.Publish");
        activity?.SetTag("messaging.message_type", messageType);
        return activity;
    }

    /// <summary>Starts a consume activity span.</summary>
    public static Activity? StartConsume(string messageType, string? messageId)
    {
        var activity = ActivitySource.StartActivity("Messaging.Consume");
        activity?.SetTag("messaging.message_type", messageType);
        activity?.SetTag("messaging.message_id", messageId);
        return activity;
    }

    /// <summary>Records a completed publish operation.</summary>
    public static void RecordPublish(string messageType, TimeSpan duration)
        => PublishDuration.Record(duration.TotalMilliseconds,
            new KeyValuePair<string, object?>("messaging.message_type", messageType));

    /// <summary>Records a publish failure.</summary>
    public static void RecordPublishError(string messageType)
        => PublishErrors.Add(1, new KeyValuePair<string, object?>("messaging.message_type", messageType));

    /// <summary>Records a completed consume operation.</summary>
    public static void RecordConsume(string messageType, TimeSpan duration)
        => ConsumeDuration.Record(duration.TotalMilliseconds,
            new KeyValuePair<string, object?>("messaging.message_type", messageType));

    /// <summary>Records a duplicate message discarded by the inbox.</summary>
    public static void RecordInboxDuplicate(string messageType)
        => InboxDuplicates.Add(1, new KeyValuePair<string, object?>("messaging.message_type", messageType));

    /// <summary>
    /// Counts one failed attempt that will be tried again.
    /// Adds a <c>messaging.retry</c> event on <see cref="Activity.Current"/> with tag <c>messaging.attempt</c>.
    /// Writes a debug log when <paramref name="logger"/> is set.
    /// A broker that calls <see cref="MessageDelivery.TryOnceAsync"/> uses this when it will redeliver.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    /// <param name="attempt">The attempt that failed. The first try is 1.</param>
    /// <param name="messageId">Transport message id, when the context has one.</param>
    /// <param name="logger">Optional logger. A debug line is written when it is set.</param>
    public static void RecordDeliveryRetry(
        string messageType,
        string subscriptionName,
        int attempt,
        string? messageId = null,
        ILogger? logger = null)
    {
        DeliveryRetries.Add(1, Tags(messageType, subscriptionName));
        logger?.LogDebug(
            "Retrying message {MessageId} of type {MessageType} after attempt {Attempt} on subscription {Subscription}",
            messageId, messageType, attempt, subscriptionName);

        Activity.Current?.AddEvent(new ActivityEvent(
            "messaging.retry",
            tags: new ActivityTagsCollection { { "messaging.attempt", attempt } }));
    }

    /// <summary>
    /// Counts one message parked after its attempts were used.
    /// Adds a <c>messaging.deadlettered</c> event on <see cref="Activity.Current"/>
    /// with the attempt count and error type, and sets that span status to <see cref="ActivityStatusCode.Error"/>.
    /// Writes a warning when <paramref name="logger"/> is set.
    /// A broker that calls <see cref="MessageDelivery.TryOnceAsync"/> uses this when it parks the message.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    /// <param name="attempts">How many times the handler ran.</param>
    /// <param name="errorType">CLR name of the last exception, or <c>Exception</c> when none was captured.</param>
    /// <param name="messageId">Transport message id, when the context has one.</param>
    /// <param name="error">Failure text from the last attempt.</param>
    /// <param name="logger">Optional logger. A warning is written when it is set.</param>
    public static void RecordDeadLettered(
        string messageType,
        string subscriptionName,
        int attempts,
        string errorType,
        string? messageId = null,
        string? error = null,
        ILogger? logger = null)
    {
        DeadLettered.Add(1, Tags(messageType, subscriptionName));
        logger?.LogWarning(
            "Dead-lettered message {MessageId} of type {MessageType} after {Attempts} attempts. {Error}",
            messageId, messageType, attempts, error);

        var tags = new ActivityTagsCollection
        {
            { "messaging.attempt", attempts },
            { "error.type", errorType },
        };
        var activity = Activity.Current;
        activity?.AddEvent(new ActivityEvent("messaging.deadlettered", tags: tags));
        activity?.SetStatus(ActivityStatusCode.Error, error);
    }

    private static KeyValuePair<string, object?>[] Tags(string messageType, string subscriptionName) =>
    [
        new("messaging.message_type", messageType),
        new("messaging.subscription_name", subscriptionName),
    ];
}
