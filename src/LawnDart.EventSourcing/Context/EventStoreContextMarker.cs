namespace LawnDart.EventSourcing.Context;

/// <summary>
/// Keyed marker registered by <c>UseInMemory</c> / <c>UseSqlServer</c> so
/// <see cref="ContextStartupValidator"/> can require <c>WithEventTypes</c>
/// without constructing the store.
/// </summary>
internal sealed class EventStoreContextMarker
{
    public static EventStoreContextMarker Instance { get; } = new();
}
