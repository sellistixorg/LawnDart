namespace LawnDart.Messaging;

/// <summary>
/// A record of a successfully processed message, stored to prevent duplicate processing.
/// </summary>
public class InboxEntry
{
    /// <summary>
    /// The unique message ID that was processed (maps to <see cref="MessageContext.MessageId"/>).
    /// </summary>
    public string MessageId { get; init; } = string.Empty;

    /// <summary>
    /// When the message was processed.
    /// </summary>
    public DateTimeOffset ProcessedAt { get; init; }
}
