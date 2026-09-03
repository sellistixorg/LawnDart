using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace LawnDart.Messaging.Telemetry;

/// <summary>
/// OpenTelemetry instrumentation for the message transport layer.
/// </summary>
public static class MessagingTelemetry
{
    internal static readonly ActivitySource ActivitySource = new("LawnDart.Messaging");
    private static readonly Meter Meter = new("LawnDart.Messaging");

    private static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
        "messaging.publish.duration", "ms", "Time taken to publish a message to the transport");

    private static readonly Histogram<double> ConsumeDuration = Meter.CreateHistogram<double>(
        "messaging.consume.duration", "ms", "Time taken to consume and dispatch a message");

    private static readonly Counter<long> PublishErrors = Meter.CreateCounter<long>(
        "messaging.publish.errors", "count", "Number of publish failures");

    private static readonly Counter<long> InboxDuplicates = Meter.CreateCounter<long>(
        "messaging.inbox.duplicates", "count", "Messages discarded due to inbox deduplication");

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
}
