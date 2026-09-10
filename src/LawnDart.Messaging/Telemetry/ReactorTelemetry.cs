using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace LawnDart.Messaging.Telemetry;

/// <summary>
/// OpenTelemetry instrumentation for reactor execution.
/// </summary>
public static class ReactorTelemetry
{
    private static readonly ActivitySource ActivitySource = new("LawnDart.Reactor");
    private static readonly Meter Meter = new("LawnDart.Reactor");

    private static readonly Histogram<double> HandleDuration = Meter.CreateHistogram<double>(
        "reactor.handle.duration", "ms", "Time taken to execute a reactor handler");

    private static readonly Counter<long> CommandsEmitted = Meter.CreateCounter<long>(
        "reactor.commands.emitted", "count", "Commands emitted by reactors");

    private static readonly Counter<long> Errors = Meter.CreateCounter<long>(
        "reactor.errors", "count", "Reactor handler errors");

    /// <summary>Starts a reactor handle activity span, continuing from inbound <c>traceparent</c>.</summary>
    public static Activity? StartHandle(string reactorType, string eventType, MessageContext? context = null)
    {
        var activity = MessageTrace.Start(ActivitySource, "Reactor.Handle", context);
        activity?.SetTag("reactor.type", reactorType);
        activity?.SetTag("reactor.event_type", eventType);
        return activity;
    }

    /// <summary>Records a completed reactor execution.</summary>
    public static void RecordHandle(string reactorType, string eventType, TimeSpan duration, int commandCount)
    {
        HandleDuration.Record(duration.TotalMilliseconds,
            new KeyValuePair<string, object?>("reactor.type", reactorType),
            new KeyValuePair<string, object?>("reactor.event_type", eventType));

        if (commandCount > 0)
        {
            CommandsEmitted.Add(commandCount,
                new KeyValuePair<string, object?>("reactor.type", reactorType));
        }
    }

    /// <summary>Records a reactor handler error.</summary>
    public static void RecordError(string reactorType, string eventType)
        => Errors.Add(1,
            new KeyValuePair<string, object?>("reactor.type", reactorType),
            new KeyValuePair<string, object?>("reactor.event_type", eventType));
}
