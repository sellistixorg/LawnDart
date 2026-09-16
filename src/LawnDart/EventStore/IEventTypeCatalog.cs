using System.Diagnostics.CodeAnalysis;

namespace LawnDart.EventStore;

/// <summary>
/// Token-only catalog used by <see cref="EventSession"/>.
/// </summary>
/// <remarks>
/// Resolve is by family token (and today's FullName / simple-name read aliases).
/// <see cref="TryResolveType(string, int, out Type)"/> accepts a schema version
/// so outbox and hydrate can pass it. Versioned catalog rows are not applied
/// yet. A context should take a scoped instance; do not treat this as a second
/// process-global dictionary to mutate from tests.
/// </remarks>
public interface IEventTypeCatalog
{
    /// <summary>Returns the family token stored for <paramref name="type"/>.</summary>
    string GetName(Type type);

    /// <summary>
    /// Resolves a stored family token (or a read alias) to a CLR type.
    /// </summary>
    bool TryResolveType(string storedName, [NotNullWhen(true)] out Type? type);

    /// <summary>
    /// Resolves a stored family token (or a read alias) at
    /// <paramref name="schemaVersion"/>.
    /// </summary>
    /// <remarks>
    /// The default implementation ignores <paramref name="schemaVersion"/> and
    /// delegates to <see cref="TryResolveType(string, out Type)"/>. Callers that
    /// have a stored version (outbox, hydrate) must use this overload so a
    /// token-only map cannot hide a versioned payload. Versioned catalog rows
    /// are not applied yet.
    /// </remarks>
    bool TryResolveType(
        string storedName,
        int schemaVersion,
        [NotNullWhen(true)] out Type? type)
        => TryResolveType(storedName, out type);
}
