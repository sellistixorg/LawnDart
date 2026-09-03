using LawnDart;

namespace LawnDart.Projections.Sdk;

/// <summary>
/// Base class for building projections with convention-based event handlers.
/// Subclasses define Handle(EventType) methods which are discovered and invoked automatically.
/// </summary>
/// <typeparam name="TView">The view/read model type being projected</typeparam>
public abstract class ProjectionBase<TView> where TView : class, new()
{
    /// <summary>
    /// The current state of the view being built
    /// </summary>
    protected TView State { get; set; } = new();

    /// <summary>
    /// The stream ID this projection instance is processing
    /// </summary>
    public string StreamId { get; internal set; } = string.Empty;

    /// <summary>
    /// The last processed event sequence position
    /// </summary>
    public long LastProcessedPosition { get; internal set; }

    /// <summary>
    /// Process an event and update the view state.
    /// This method uses reflection to find and invoke the appropriate Handle method.
    /// </summary>
    /// <param name="event">The event to process</param>
    /// <param name="sequencePosition">The global sequence position of the event</param>
    public virtual void ProcessEvent(IEvent @event, long sequencePosition)
    {
        var eventType = @event.GetType();
        var handleMethod = GetType().GetMethod("Handle", new[] { eventType });

        if (handleMethod != null)
        {
            handleMethod.Invoke(this, new object[] { @event });
            LastProcessedPosition = sequencePosition;
        }
        else
        {
            // No handler found - this is fine, projection might not care about this event
        }
    }

    /// <summary>
    /// Gets the current view state
    /// </summary>
    public virtual TView GetView() => State;

    /// <summary>
    /// Resets the projection state (used for rebuilds)
    /// </summary>
    public virtual void Reset()
    {
        State = new TView();
        LastProcessedPosition = 0;
    }

    /// <summary>
    /// Override this to customize view state initialization
    /// </summary>
    protected virtual void OnInitialize() { }

    /// <summary>
    /// Override this to perform cleanup when projection is stopped
    /// </summary>
    protected virtual void OnShutdown() { }
}
