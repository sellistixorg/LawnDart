namespace LawnDart.EventStore;

/// <summary>
/// An event store that wakes same-process readers after a committed save.
/// </summary>
public interface ISignalsNewEvents
{
    /// <summary>
    /// Shared wake-up for this store. The store calls <see cref="NewEventsSignal.Notify"/>
    /// after a save commits. A listener outside the store calls
    /// <see cref="NewEventsSignal.Notify"/> on this same object when another
    /// process has committed events.
    /// </summary>
    NewEventsSignal NewEvents { get; }
}
