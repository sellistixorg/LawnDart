namespace LawnDart.Messaging;

/// <summary>
/// A record of a successfully processed message, stored to prevent duplicate processing.
/// </summary>
public class InboxEntry
{
    /// <summary>
    /// Dedup key that was processed. Hosted consumers store the consumer type name plus
    /// <see cref="MessageContext.MessageId"/>.
    /// </summary>
    public string MessageId { get; init; } = string.Empty;

    /// <summary>
    /// When the message was processed.
    /// </summary>
    public DateTimeOffset ProcessedAt { get; init; }
}
