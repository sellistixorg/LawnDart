using LawnDart.Metadata;

namespace LawnDart;

/// <summary>
/// Provides metadata capture and enrichment capabilities for commands and events.
/// </summary>
public interface IMetadataProvider
{
    /// <summary>
    /// Captures metadata from the current execution context (HTTP, messaging, background).
    /// Reads <c>Activity.Current</c> and ambient <c>MessageContext</c> when present;
    /// otherwise mints a correlation id.
    /// </summary>
    /// <param name="context">Optional context object (e.g., HttpContext, ServiceBusReceivedMessage).</param>
    /// <returns>Command metadata captured from the context.</returns>
    CommandMetadata CaptureCommandMetadata(object? context = null);

    /// <summary>
    /// Enriches event metadata with command metadata, adding event-specific fields.
    /// </summary>
    /// <param name="baseMetadata">Base event metadata (may be empty).</param>
    /// <param name="commandMetadata">Command metadata to inherit from.</param>
    /// <param name="event">The event being enriched.</param>
    /// <returns>Enriched event metadata.</returns>
    EventMetadata EnrichEventMetadata(
        EventMetadata baseMetadata,
        CommandMetadata commandMetadata,
        IEvent @event);
}


