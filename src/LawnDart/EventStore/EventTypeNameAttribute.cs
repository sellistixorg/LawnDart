namespace LawnDart.EventStore;

/// <summary>
/// Declares the stable catalog token stored in the event log for this type.
/// Required on every concrete <see cref="IEvent"/> that is written.
/// Prefer a kebab-case token (e.g. <c>"order-placed"</c>).
/// </summary>
/// <remarks>
/// <see cref="EventTypeNameResolver.GetName"/> does not fall back to CLR
/// <c>FullName</c>. Register types with <c>WithEventTypes</c> or
/// <see cref="EventTypeNameResolver.Warmup"/> so duplicate tokens fail at startup
/// and older FullName rows can still be resolved as read aliases.
/// </remarks>
/// <example>
/// <code>
/// // Preserve the short name that was written to the event store before the
/// // namespace was introduced, avoiding any data migration.
/// [EventTypeName("OrderPlaced")]
/// public record OrderPlaced(...) : IEvent { ... }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class EventTypeNameAttribute : Attribute
{
    /// <summary>
    /// The stable alias that will be stored in the event log for this event type.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Initializes the attribute with the given stable alias.
    /// </summary>
    /// <param name="name">
    /// The stable alias to store (e.g. <c>"order-placed"</c> or <c>"OrderPlaced"</c>).
    /// Must be unique across all event types registered with the event store.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="name"/> is <c>null</c>.</exception>
    public EventTypeNameAttribute(string name)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
    }
}
