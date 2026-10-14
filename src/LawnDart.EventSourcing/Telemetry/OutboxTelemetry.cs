using System.Diagnostics.Metrics;

namespace LawnDart.EventSourcing.Telemetry;

/// <summary>
/// OpenTelemetry counters for the outbox processor.
/// </summary>
public static class OutboxTelemetry
{
    /// <summary>Meter name. Value is <c>LawnDart.Outbox</c>.</summary>
    public const string MeterName = "LawnDart.Outbox";

    private static readonly Meter Meter = new(MeterName);

    private static readonly Counter<long> PublishFailures = Meter.CreateCounter<long>(
        "outbox.publish.failures", "count", "Outbox publish attempts that failed");

    private static readonly Counter<long> DeadLettered = Meter.CreateCounter<long>(
        "outbox.deadlettered", "count", "Outbox messages parked after they used their attempts");

    /// <summary>Counts one failed publish attempt.</summary>
    /// <param name="eventType">Family token stored on the outbox row.</param>
    public static void RecordPublishFailure(string? eventType)
        => PublishFailures.Add(1, new KeyValuePair<string, object?>("outbox.event_type", eventType));

    /// <summary>Counts one outbox row parked after its attempts were used.</summary>
    /// <param name="eventType">Family token stored on the outbox row.</param>
    public static void RecordDeadLettered(string? eventType)
        => DeadLettered.Add(1, new KeyValuePair<string, object?>("outbox.event_type", eventType));
}
