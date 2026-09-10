using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace LawnDart.Messaging.Telemetry;

/// <summary>
/// OpenTelemetry instrumentation for event processor execution.
/// </summary>
public static class EventProcessorTelemetry
{
    private static readonly ActivitySource ActivitySource = new("LawnDart.EventProcessor");
    private static readonly Meter Meter = new("LawnDart.EventProcessor");

    private static readonly Histogram<double> ProcessDuration = Meter.CreateHistogram<double>(
        "event_processor.process.duration", "ms", "Time taken to process an event");

    private static readonly Counter<long> EventsEmitted = Meter.CreateCounter<long>(
        "event_processor.events.emitted", "count", "Derived events emitted by processors");

    private static readonly Counter<long> Errors = Meter.CreateCounter<long>(
        "event_processor.errors", "count", "Event processor handler errors");

    /// <summary>Starts an event processor activity span, continuing from inbound <c>traceparent</c>.</summary>
    public static Activity? StartProcess(string processorType, string eventType, MessageContext? context = null)
    {
        var activity = MessageTrace.Start(ActivitySource, "EventProcessor.Process", context);
        activity?.SetTag("event_processor.type", processorType);
        activity?.SetTag("event_processor.event_type", eventType);
        return activity;
    }

    /// <summary>Records a completed processor execution.</summary>
    public static void RecordProcess(string processorType, string eventType, TimeSpan duration, int eventCount)
    {
        ProcessDuration.Record(duration.TotalMilliseconds,
            new KeyValuePair<string, object?>("event_processor.type", processorType),
            new KeyValuePair<string, object?>("event_processor.event_type", eventType));

        if (eventCount > 0)
        {
            EventsEmitted.Add(eventCount,
                new KeyValuePair<string, object?>("event_processor.type", processorType));
        }
    }

    /// <summary>Records an event processor handler error.</summary>
    public static void RecordError(string processorType, string eventType)
        => Errors.Add(1,
            new KeyValuePair<string, object?>("event_processor.type", processorType),
            new KeyValuePair<string, object?>("event_processor.event_type", eventType));
}
