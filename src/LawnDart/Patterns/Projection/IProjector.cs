using System.Diagnostics.CodeAnalysis;

namespace LawnDart.Patterns.Projection;

/// <summary>
/// Optional fold contract (Event → State). Not the authoring API.
/// </summary>
/// <remarks>
/// Lightweight does not discover or call this type. Author
/// <c>ProjectionBase&lt;TView&gt;</c> plus scope attributes. Multi-stream
/// views (including Flywheel) implement <c>IMultiStreamEntityResolver</c>
/// on that handler. Implement this interface only if you hand-roll a fold
/// against <c>IEventStore</c>.
/// </remarks>
/// <typeparam name="TState">The state type being projected.</typeparam>
[Experimental("LAWNDART001")]
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
