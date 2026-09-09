using System.Diagnostics.CodeAnalysis;

namespace LawnDart.Patterns.EventGenerator;

/// <summary>
/// Generates events from current state (State → Event pattern).
/// </summary>
/// <remarks>
/// Planned cell (🔧). No host in this version. Implementing this
/// interface does not register it with DI or cause the runtime to
/// invoke it.
/// </remarks>
/// <typeparam name="TState">The state type to inspect.</typeparam>
[Experimental("LAWNDART002")]
public interface IEventGenerator<in TState> where TState : IState
{
    /// <summary>
    /// Inspects the given state and produces any events that should be raised.
    /// </summary>
    /// <param name="state">Current state to evaluate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Events to publish, or an empty enumerable.</returns>
    Task<IEnumerable<IEvent>> GenerateAsync(TState state, CancellationToken cancellationToken = default);
}
