namespace LawnDart.EventStore;

/// <summary>
/// Converts a stored historical event to a later schema in the same family.
/// Register implementations with <c>WithUpcasters</c>. There is no downcast API.
/// </summary>
/// <typeparam name="TTo">The later CLR type in the family.</typeparam>
/// <typeparam name="TFrom">The stored historical CLR type.</typeparam>
/// <remarks>
/// <para>
/// One hop. A v1 → v3 family is either a direct upcaster or a chain
/// (v1 → v2, v2 → v3). <see cref="EventUpcastPipeline.Materialize"/> fails
/// if any historical version cannot reach current.
/// </para>
/// <para>
/// Warmup is the runtime authority for a complete chain. Typed hydrate
/// applies the chain on read.
/// </para>
/// </remarks>
public interface IEventUpcaster<out TTo, in TFrom>
    where TTo : class, IEvent
    where TFrom : class, IEvent
{
    /// <summary>Maps <paramref name="source"/> to the later schema.</summary>
    TTo Upcast(TFrom source);
}
