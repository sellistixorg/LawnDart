namespace LawnDart.Projections.Lightweight;

/// <summary>
/// Implemented by a <see cref="LawnDart.Projections.Sdk.ProjectionBase{TView}"/> class
/// decorated with <see cref="MultiStreamProjectionAttribute"/> to extract the logical entity ID
/// from an incoming event.
/// </summary>
/// <remarks>
/// The entity ID groups events from multiple stream types into a single view instance.
/// For example, an order fulfilment view consuming events from both <c>OrderAggregate</c> and
/// <c>ShipmentAggregate</c> streams would return the shared <c>OrderId</c> as the entity ID.
///
/// Return <see langword="null"/> from <see cref="GetEntityId"/> if the event is not relevant to
/// any entity — it will be silently skipped.
///
/// <example>
/// <code>
/// public string? GetEntityId(IEvent e) => e switch
/// {
///     OrderPlaced  op => op.OrderId.ToString(),
///     OrderShipped os => os.OrderId.ToString(),
///     _               => null
/// };
/// </code>
/// </example>
/// </remarks>
public interface IMultiStreamEntityResolver
{
    /// <summary>
    /// Extracts the entity identifier from the given event.
    /// </summary>
    /// <param name="event">The event to inspect.</param>
    /// <returns>
    /// A non-empty string identifying the view instance, or <see langword="null"/> to skip
    /// this event entirely.
    /// </returns>
    string? GetEntityId(LawnDart.IEvent @event);
}
