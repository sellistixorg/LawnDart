using System.Diagnostics.CodeAnalysis;

namespace LawnDart.EventStore;

/// <summary>
/// Scoped wrapper over <see cref="EventTypeNameResolver"/>.
/// </summary>
/// <remarks>
/// Instances can be registered per bounded context. The backing maps are still
/// the process-wide resolver until a materialized catalog replaces this adapter.
/// </remarks>
public sealed class EventTypeCatalog : IEventTypeCatalog
{
    /// <summary>
    /// Shared adapter over <see cref="EventTypeNameResolver"/>. Prefer injecting
    /// a scoped instance rather than reaching for this from application code.
    /// </summary>
    public static EventTypeCatalog Shared { get; } = new();

    /// <inheritdoc />
    public string GetName(Type type) => EventTypeNameResolver.GetName(type);

    /// <inheritdoc />
    public bool TryResolveType(string storedName, [NotNullWhen(true)] out Type? type)
        => EventTypeNameResolver.TryResolveType(storedName, out type);

    /// <inheritdoc />
    public bool TryResolveType(string storedName, int schemaVersion, [NotNullWhen(true)] out Type? type)
        => TryResolveType(storedName, out type);
}
