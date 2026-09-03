namespace LawnDart.Tagging;

/// <summary>
/// Provides tags for events, enabling custom tagging strategies.
/// </summary>
public interface ITagProvider
{
    /// <summary>
    /// Gets tags for an event.
    /// </summary>
    /// <param name="event">The event to tag.</param>
    /// <param name="context">Optional context (e.g., aggregate, command metadata).</param>
    /// <returns>Tags to apply to the event.</returns>
    IEnumerable<string> GetTags(IEvent @event, object? context = null);
}


