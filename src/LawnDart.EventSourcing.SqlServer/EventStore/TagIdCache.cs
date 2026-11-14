using System.Collections.Concurrent;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// Process-local map from tag text to the id stored in <c>TagRegistry</c>.
/// An id does not change once it is assigned.
/// </summary>
internal sealed class TagIdCache
{
    private readonly ConcurrentDictionary<string, int> _tagToId = new(StringComparer.Ordinal);

    public bool TryGetId(string tag, out int id)
        => _tagToId.TryGetValue(tag, out id);

    public int? FindId(string tag)
        => _tagToId.TryGetValue(tag, out var id) ? id : null;

    public void Add(int id, string tag)
        => _tagToId[tag] = id;
}
