namespace LawnDart.EventStore;

/// <summary>
/// Converts a stored historical event to a later schema for the same event name.
/// Register implementations with <c>WithUpcasters</c>. There is no downcast API.
/// </summary>
/// <typeparam name="TTo">The later .NET type for the event name.</typeparam>
/// <typeparam name="TFrom">The stored historical .NET type.</typeparam>
/// <remarks>
/// <para>
/// One hop. An event name that moves from version 1 to version 3 is either a
/// direct upcaster or a chain (version 1 to version 2, version 2 to version 3).
/// <see cref="EventUpcastPipeline.Materialize"/> throws
/// if any historical version cannot reach current.
/// </para>
/// <para>
/// Warmup is the runtime authority for a complete chain. A typed read
/// applies the chain.
/// </para>
/// </remarks>
public interface IEventUpcaster<out TTo, in TFrom>
    where TTo : class, IEvent
    where TFrom : class, IEvent
{
    /// <summary>Maps <paramref name="source"/> to the later schema.</summary>
    TTo Upcast(TFrom source);
}
