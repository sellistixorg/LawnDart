namespace LawnDart.Messaging;

/// <summary>
/// Carrier for cross-process correlation, causation, and deduplication metadata.
/// Propagated through the message pipeline alongside every incoming event or command.
/// </summary>
public class MessageContext
{
    /// <summary>
    /// Unique identifier for this message. Used as the idempotency key for inbox deduplication.
    /// </summary>
    public string? MessageId { get; init; }

    /// <summary>
    /// Correlation identifier that groups all messages belonging to the same logical operation or saga.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Identifier of the message or command that directly caused this message to be produced.
    /// </summary>
    public string? CausationId { get; init; }

    /// <summary>
    /// Tenant context propagated from the originating service.
    /// </summary>
    public string? TenantId { get; init; }

    /// <summary>
    /// User who initiated the original operation that produced this message.
    /// </summary>
    public string? UserId { get; init; }

    /// <summary>
    /// When the message was enqueued by the publishing service.
    /// </summary>
    public DateTimeOffset EnqueuedAt { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Transport type that delivered this message (e.g. "InMemory", "ServiceBus").
    /// </summary>
    public string? TransportType { get; init; }

    /// <summary>
    /// Additional transport-level or application-level headers.
    /// </summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// Creates an empty context with a generated message ID.
    /// </summary>
    public static MessageContext New() => new() { MessageId = Guid.NewGuid().ToString() };

    /// <summary>
    /// Creates a context derived from this one, suitable for a reply or downstream command.
    /// The new context has its own message ID, with this message's ID as its causation ID.
    /// </summary>
    public MessageContext CreateChild() => new()
    {
        MessageId = Guid.NewGuid().ToString(),
        CorrelationId = CorrelationId ?? MessageId,
        CausationId = MessageId,
        TenantId = TenantId,
        UserId = UserId,
        TransportType = TransportType,
        Headers = MessageTrace.WithCurrentTraceHeaders(Headers),
    };
}
