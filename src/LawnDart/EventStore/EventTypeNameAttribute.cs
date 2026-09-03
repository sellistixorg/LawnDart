namespace LawnDart.EventStore;

/// <summary>
/// Declares a stable, human-readable alias for an event type that is stored in the event log
/// instead of the CLR <c>FullName</c>. Use this attribute when:
/// <list type="bullet">
///   <item><description>The class or namespace may be renamed in a future refactor.</description></item>
///   <item><description>You want a compact, domain-meaningful token on disk (e.g. <c>"order-placed"</c>).</description></item>
///   <item><description>You need backward compatibility with data already written under a short name.</description></item>
/// </list>
/// </summary>
/// <remarks>
/// When this attribute is absent the <see cref="EventTypeNameResolver"/> falls back to
/// <c>Type.FullName</c> (namespace-qualified class name), which is stable as long as the
/// namespace and class name do not change.  If <c>FullName</c> is <c>null</c> (e.g. for
/// anonymous types) <c>Type.Name</c> is used as a last resort.
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
