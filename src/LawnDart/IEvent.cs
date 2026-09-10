namespace LawnDart;

/// <summary>
/// Represents an event - something that has happened (past fact).
/// </summary>
public interface IEvent
{
    /// <summary>
    /// Unique identifier for the event.
    /// </summary>
    Guid Id { get; }

    /// <summary>
    /// Timestamp when the event occurred.
    /// </summary>
    DateTime Timestamp { get; }
}


