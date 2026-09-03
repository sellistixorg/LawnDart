using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace LawnDart.Messaging.Telemetry;

/// <summary>
/// OpenTelemetry instrumentation for task processor execution.
/// </summary>
public static class TaskProcessorTelemetry
{
    private static readonly ActivitySource ActivitySource = new("LawnDart.TaskProcessor");
    private static readonly Meter Meter = new("LawnDart.TaskProcessor");

    private static readonly Histogram<double> PollDuration = Meter.CreateHistogram<double>(
        "task_processor.poll.duration", "ms", "Time taken to execute a task processor poll");

    private static readonly Counter<long> CommandsEmitted = Meter.CreateCounter<long>(
        "task_processor.commands.emitted", "count", "Commands emitted by task processors");

    private static readonly Counter<long> Errors = Meter.CreateCounter<long>(
        "task_processor.errors", "count", "Task processor poll errors");

    /// <summary>Starts a task processor poll activity span.</summary>
    public static Activity? StartPoll(string processorType)
    {
        var activity = ActivitySource.StartActivity("TaskProcessor.Poll");
        activity?.SetTag("task_processor.type", processorType);
        return activity;
    }

    /// <summary>Records a completed poll cycle.</summary>
    public static void RecordPoll(string processorType, TimeSpan duration, int commandCount)
    {
        PollDuration.Record(duration.TotalMilliseconds,
            new KeyValuePair<string, object?>("task_processor.type", processorType));

        if (commandCount > 0)
        {
            CommandsEmitted.Add(commandCount,
                new KeyValuePair<string, object?>("task_processor.type", processorType));
        }
    }

    /// <summary>Records a task processor poll error.</summary>
    public static void RecordError(string processorType)
        => Errors.Add(1, new KeyValuePair<string, object?>("task_processor.type", processorType));
}
