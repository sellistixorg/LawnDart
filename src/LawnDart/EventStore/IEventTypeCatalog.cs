using System.Diagnostics.CodeAnalysis;

namespace LawnDart.EventStore;

/// <summary>
/// Catalog used by <see cref="EventSession"/>: family token plus schema version.
/// </summary>
/// <remarks>
/// Resolve-on-read is <c>(token, SchemaVersion)</c>. Token-only resolve returns
/// the family's current type (or a FullName / simple-name / AssemblyQualifiedName
/// read alias). A context should take a scoped instance from
/// <see cref="EventTypeCatalog.Materialize"/>; do not treat
/// <see cref="EventTypeCatalog.Shared"/> as a second process-global dictionary
/// to mutate from tests.
/// </remarks>
public interface IEventTypeCatalog
{
    /// <summary>Returns the family token stored for <paramref name="type"/>.</summary>
    string GetName(Type type);

    /// <summary>
    /// Resolves a stored family token to the current CLR type, or a read alias
    /// (FullName / simple name / AssemblyQualifiedName) to that type.
    /// </summary>
    bool TryResolveType(string storedName, [NotNullWhen(true)] out Type? type);

    /// <summary>
    /// Resolves a stored family token (or a read alias) at
    /// <paramref name="schemaVersion"/>. Missing or zero version treats as 1.
    /// A known family with no CLR type for that version returns false.
    /// </summary>
    bool TryResolveType(
        string storedName,
        int schemaVersion,
        [NotNullWhen(true)] out Type? type)
        => TryResolveType(storedName, out type);
}
