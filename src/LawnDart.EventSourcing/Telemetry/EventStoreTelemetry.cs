using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Telemetry;

/// <summary>
/// Telemetry for event store operations using ActivitySource and metrics.
/// </summary>
public static class EventStoreTelemetry
{
    private static readonly ActivitySource ActivitySource = new("LawnDart.EventStore");
    private static readonly Meter Meter = new("LawnDart.EventStore");

    private static readonly Counter<long> EventsAppendedCounter = Meter.CreateCounter<long>(
        "lawndart_events_appended_total",
        "events",
        "Total number of events appended to the event store");

    private static readonly Counter<long> EventsReadCounter = Meter.CreateCounter<long>(
        "lawndart_events_read_total",
        "events",
        "Total number of events read from the event store");

    private static readonly Histogram<double> AppendDuration = Meter.CreateHistogram<double>(
        "lawndart_append_duration_seconds",
        "seconds",
        "Duration of append operations");

    private static readonly Histogram<double> ReadDuration = Meter.CreateHistogram<double>(
        "lawndart_read_duration_seconds",
        "seconds",
        "Duration of read operations");

    /// <summary>
    /// Gets the ActivitySource for distributed tracing.
    /// </summary>
    public static ActivitySource GetActivitySource() => ActivitySource;

    /// <summary>
    /// Gets the Meter for metrics.
    /// </summary>
    public static Meter GetMeter() => Meter;

    /// <summary>
    /// Creates an activity for an append operation.
    /// </summary>
    public static Activity? StartAppendActivity(string streamId, int eventCount)
    {
        var activity = ActivitySource.StartActivity("EventStore.Append");
        if (activity != null)
        {
            activity.SetTag("lawndart.stream_id", streamId);
            activity.SetTag("lawndart.event_count", eventCount);
        }
        return activity;
    }

    /// <summary>
    /// Records metrics for an append operation.
    /// </summary>
    public static void RecordAppend(string streamId, int eventCount, TimeSpan duration, bool success)
    {
        EventsAppendedCounter.Add(eventCount, new KeyValuePair<string, object?>("stream_id", streamId));
        AppendDuration.Record(duration.TotalSeconds, new KeyValuePair<string, object?>("stream_id", streamId));
        
        if (!success)
        {
            Meter.CreateCounter<long>("lawndart_append_errors_total").Add(1, new KeyValuePair<string, object?>("stream_id", streamId));
        }
    }

    /// <summary>
    /// Creates an activity for a read operation.
    /// </summary>
    public static Activity? StartReadActivity(string operation, string? streamId = null)
    {
        var activity = ActivitySource.StartActivity($"EventStore.Read.{operation}");
        if (activity != null)
        {
            if (streamId != null)
            {
                activity.SetTag("lawndart.stream_id", streamId);
            }
        }
        return activity;
    }

    /// <summary>
    /// Records metrics for a read operation.
    /// </summary>
    public static void RecordRead(string operation, int eventCount, TimeSpan duration, string? streamId = null)
    {
        EventsReadCounter.Add(eventCount, 
            new KeyValuePair<string, object?>("operation", operation),
            new KeyValuePair<string, object?>("stream_id", streamId ?? "unknown"));
        ReadDuration.Record(duration.TotalSeconds,
            new KeyValuePair<string, object?>("operation", operation),
            new KeyValuePair<string, object?>("stream_id", streamId ?? "unknown"));
    }
}

