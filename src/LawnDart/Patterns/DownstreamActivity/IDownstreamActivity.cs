namespace LawnDart.Patterns.DownstreamActivity;

/// <summary>
/// Executes a downstream activity triggered by a command or event (Command/Event → State pattern).
/// </summary>
/// <typeparam name="TTrigger">The trigger type (command or event).</typeparam>
public interface IDownstreamActivity<in TTrigger> where TTrigger : IMessage
{
    /// <summary>
    /// Executes the downstream activity.
    /// </summary>
    /// <param name="trigger">The command or event that triggered the activity.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task representing the async operation.</returns>
    Task ExecuteAsync(TTrigger trigger, CancellationToken cancellationToken = default);
}


