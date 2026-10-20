using System.Collections.Concurrent;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// Process-local map from event name to the small integer stored on each row.
/// A miss reloads the table before a query is built.
/// </summary>
internal sealed class EventTypeIdCache
{
    private readonly ConcurrentDictionary<string, int> _tokenToId = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<int, string> _idToToken = new();

    /// <summary>Returns the id for <paramref name="token"/> when this process has seen it.</summary>
    public int? FindId(string token)
        => _tokenToId.TryGetValue(token, out var id) ? id : null;

    /// <summary>Returns the event name for <paramref name="id"/> when this process has seen it.</summary>
    public bool TryGetToken(int id, out string token)
        => _idToToken.TryGetValue(id, out token!);

    /// <summary>Records one pair.</summary>
    public void Add(int id, string token)
    {
        _tokenToId[token] = id;
        _idToToken[id] = token;
    }

    /// <summary>Replaces the map with the rows just read from the database.</summary>
    public void ReplaceAll(IEnumerable<(int Id, string Token)> rows)
    {
        _tokenToId.Clear();
        _idToToken.Clear();
        foreach (var (id, token) in rows)
            Add(id, token);
    }
}
