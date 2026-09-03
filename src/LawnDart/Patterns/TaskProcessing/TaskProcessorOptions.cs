namespace LawnDart.Patterns.TaskProcessing;

/// <summary>
/// Configuration for a task processor's polling behaviour.
/// Registered per-processor via <c>AddTaskProcessor&lt;T&gt;()</c>.
/// </summary>
public class TaskProcessorOptions
{
    /// <summary>
    /// How frequently the task processor polls for work.
    /// Defaults to 60 seconds.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Maximum number of consecutive errors before the polling loop backs off.
    /// Defaults to 5.
    /// </summary>
    public int ErrorBackoffThreshold { get; set; } = 5;

    /// <summary>
    /// How long to pause polling after exceeding <see cref="ErrorBackoffThreshold"/>.
    /// Defaults to 5 minutes.
    /// </summary>
    public TimeSpan ErrorBackoffDuration { get; set; } = TimeSpan.FromMinutes(5);
}
