namespace LawnDart.Patterns.Projection;

/// <summary>
/// Projects events into state (Event → State pattern).
/// </summary>
/// <typeparam name="TState">The state type being projected.</typeparam>
public interface IProjector<TState> where TState : IState
{
    /// <summary>
    /// Projects an event into the state.
    /// </summary>
    /// <typeparam name="TEvent">The event type.</typeparam>
    /// <param name="event">The event to project.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task representing the async operation.</returns>
    Task ProjectAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default) where TEvent : IEvent;
}


