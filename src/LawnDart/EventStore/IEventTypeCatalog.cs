using System.Diagnostics.CodeAnalysis;

namespace LawnDart.EventStore;

/// <summary>
/// Catalog used by <see cref="EventSession"/>: event name plus schema version.
/// </summary>
/// <remarks>
/// Resolve-on-read is <c>(token, SchemaVersion)</c>. A name-only resolve returns
/// the current type for that event name. A context takes a scoped instance from
/// <see cref="EventTypeCatalog.Materialize"/> via <c>WithEventTypes</c>.
/// There is no process-wide catalog and no FullName / simple-name /
/// AssemblyQualifiedName read alias.
/// </remarks>
public interface IEventTypeCatalog
{
    /// <summary>Returns the event name stored for <paramref name="type"/>.</summary>
    string GetName(Type type);

    /// <summary>
    /// Resolves a stored event name to the current .NET type.
    /// </summary>
    bool TryResolveType(string storedName, [NotNullWhen(true)] out Type? type);

    /// <summary>
    /// Resolves a stored event name at <paramref name="schemaVersion"/>.
    /// Missing or zero version treats as 1. A known event name with no .NET type
    /// for that version returns false. Does not ignore <paramref name="schemaVersion"/>.
    /// </summary>
    bool TryResolveType(
        string storedName,
        int schemaVersion,
        [NotNullWhen(true)] out Type? type)
        => TryResolveType(storedName, out type);
}
