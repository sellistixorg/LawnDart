namespace LawnDart.Publishing;

/// <summary>
/// What a translation may read besides the internal event itself.
/// </summary>
/// <remarks>
/// No services and no store. The values come from the internal event and the
/// metadata of the append that carries it.
/// </remarks>
/// <param name="SourceEventId">Id of the internal event. This is also the outbox message id.</param>
/// <param name="SourceEventType">Stored event name of the internal event.</param>
/// <param name="Tags">Tags written on the internal event.</param>
/// <param name="TenantId">Tenant of the append, when set.</param>
/// <param name="CorrelationId">Correlation id of the append, when set.</param>
/// <param name="CausationId">Causation id of the append, when set.</param>
/// <param name="Timestamp">Business time of the internal event (<see cref="IEvent.Timestamp"/>).</param>
public readonly record struct PublishContext(
    Guid SourceEventId,
    string SourceEventType,
    IReadOnlyList<string> Tags,
    string? TenantId,
    string? CorrelationId,
    string? CausationId,
    DateTime Timestamp);
