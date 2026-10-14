namespace LawnDart.Messaging;

/// <summary>
/// Header names for a message envelope.
/// Transports write these names on the wire and restore them onto <see cref="MessageContext"/>.
/// </summary>
public static class MessageHeaders
{
    /// <summary>Header for <see cref="MessageContext.MessageId"/>.</summary>
    public const string MessageId = "message-id";

    /// <summary>Header for the declared message type name.</summary>
    public const string MessageType = "message-type";

    /// <summary>Header for <see cref="MessageContext.CorrelationId"/>.</summary>
    public const string CorrelationId = "correlation-id";

    /// <summary>Header for <see cref="MessageContext.CausationId"/>.</summary>
    public const string CausationId = "causation-id";

    /// <summary>Header for <see cref="MessageContext.UserId"/>.</summary>
    public const string UserId = "user-id";

    /// <summary>Header for <see cref="MessageContext.TenantId"/>.</summary>
    public const string TenantId = "tenant-id";

    /// <summary>Header for <see cref="MessageContext.EnqueuedAt"/>, as a round-trip timestamp.</summary>
    public const string EnqueuedAt = "enqueued-at";

    /// <summary>
    /// Prefix for application headers on the wire.
    /// <see cref="MessageEnvelopeHeaders"/> adds it when writing and removes it when reading.
    /// </summary>
    public const string CustomPrefix = "header.";

    /// <summary>Outbox header for the stream id. Value is <c>outbox.streamId</c>.</summary>
    public const string OutboxStreamId = "outbox.streamId";

    /// <summary>Outbox header for the sequence position. Value is <c>outbox.sequencePosition</c>.</summary>
    public const string OutboxSequencePosition = "outbox.sequencePosition";

    /// <summary>Outbox header for the event type token. Value is <c>outbox.eventType</c>.</summary>
    public const string OutboxEventType = "outbox.eventType";

    /// <summary>W3C <c>traceparent</c> header.</summary>
    public const string TraceParent = MessageTrace.TraceParentHeader;

    /// <summary>W3C <c>tracestate</c> header.</summary>
    public const string TraceState = MessageTrace.TraceStateHeader;
}
