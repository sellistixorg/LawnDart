using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace LawnDart.Dcb.Telemetry;

/// <summary>
/// Telemetry for DCB operations.
/// </summary>
public static class DcbTelemetry
{
    private static readonly ActivitySource ActivitySource = new("LawnDart.Dcb");
    private static readonly Meter Meter = new("LawnDart.Dcb");
    
    private static readonly Histogram<double> GetStateDuration = Meter.CreateHistogram<double>(
        "dcb.repository.get_state.duration",
        "ms",
        "Duration of GetState operations");
    
    private static readonly Histogram<long> GetStateEventCount = Meter.CreateHistogram<long>(
        "dcb.repository.get_state.event_count",
        "count",
        "Number of events loaded in GetState");
    
    private static readonly Histogram<double> AppendDuration = Meter.CreateHistogram<double>(
        "dcb.repository.append.duration",
        "ms",
        "Duration of Append operations");
    
    private static readonly Counter<long> EmitCount = Meter.CreateCounter<long>(
        "dcb.entity.emit.count",
        "count",
        "Number of events emitted by entities");
    
    private static readonly Histogram<double> HandleCommandDuration = Meter.CreateHistogram<double>(
        "dcb.repository.handle_command.duration",
        "ms",
        "Duration of HandleCommand operations");
    
    /// <summary>
    /// Starts a GetState activity.
    /// </summary>
    public static Activity? StartGetStateActivity(string stateType, string[] tags)
    {
        var activity = ActivitySource.StartActivity("DcbGetState");
        activity?.SetTag("state.type", stateType);
        activity?.SetTag("tags.count", tags.Length);
        return activity;
    }
    
    /// <summary>
    /// Starts an Append activity.
    /// </summary>
    public static Activity? StartAppendActivity(int eventCount)
    {
        var activity = ActivitySource.StartActivity("DcbAppend");
        activity?.SetTag("event.count", eventCount);
        return activity;
    }
    
    /// <summary>
    /// Starts a HandleCommand activity.
    /// </summary>
    public static Activity? StartHandleCommandActivity(string commandType)
    {
        var activity = ActivitySource.StartActivity("DcbHandleCommand");
        activity?.SetTag("command.type", commandType);
        return activity;
    }
    
    /// <summary>
    /// Records GetState metrics.
    /// </summary>
    public static void RecordGetState(int eventCount, TimeSpan duration)
    {
        GetStateDuration.Record(duration.TotalMilliseconds);
        GetStateEventCount.Record(eventCount);
    }
    
    /// <summary>
    /// Records Append metrics.
    /// </summary>
    public static void RecordAppend(int eventCount, TimeSpan duration)
    {
        AppendDuration.Record(duration.TotalMilliseconds);
    }
    
    /// <summary>
    /// Records entity emit.
    /// </summary>
    public static void RecordEmit()
    {
        EmitCount.Add(1);
    }
    
    /// <summary>
    /// Records HandleCommand metrics.
    /// </summary>
    public static void RecordHandleCommand(string commandType, TimeSpan duration)
    {
        HandleCommandDuration.Record(duration.TotalMilliseconds, new KeyValuePair<string, object?>("command.type", commandType));
    }
}
