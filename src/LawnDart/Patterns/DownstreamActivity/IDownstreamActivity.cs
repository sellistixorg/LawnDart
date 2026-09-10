using System.Diagnostics.CodeAnalysis;

namespace LawnDart.Patterns.DownstreamActivity;

/// <summary>
/// Executes a downstream activity triggered by a command or event (Command/Event → State pattern).
/// </summary>
/// <remarks>
/// Planned cell (🔧). No host in this version. Implementing this
/// interface does not register it with DI or cause the runtime to
/// invoke it.
/// </remarks>
/// <typeparam name="TTrigger">The trigger type (command or event).</typeparam>
[Experimental("LAWNDART002")]
public interface IDownstreamActivity<in TTrigger>
{
    /// <summary>
    /// Executes the downstream activity.
    /// </summary>
    /// <param name="trigger">The command or event that triggered the activity.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task representing the async operation.</returns>
    Task ExecuteAsync(TTrigger trigger, CancellationToken cancellationToken = default);
}
