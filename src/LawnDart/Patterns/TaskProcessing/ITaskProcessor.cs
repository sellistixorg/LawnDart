namespace LawnDart.Patterns.TaskProcessing;

/// <summary>
/// Scheduled task processor: polls state on a configured interval and emits commands in response
/// (State → Command pattern). A hosted service wrapper drives the polling loop and dispatches
/// the returned commands. Idempotency must be ensured by the implementation (e.g. by tracking
/// last-processed position or using a lease/lock mechanism).
/// </summary>
public interface ITaskProcessor
{
    /// <summary>
    /// Polls current state and produces commands to execute.
    /// Called repeatedly by the hosting infrastructure on the configured polling interval.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token signalled when the host is stopping.</param>
    /// <returns>Commands to dispatch, or an empty enumerable when no action is needed.</returns>
    Task<IEnumerable<ICommand>> ProcessTasksAsync(CancellationToken cancellationToken = default);
}
