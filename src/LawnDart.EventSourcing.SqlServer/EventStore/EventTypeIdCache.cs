using System.Collections.Concurrent;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// Process-local token↔id map for <c>EventTypes</c>. Reads resolve from here
/// and never join the lookup table. A miss is a refresh, then fail-closed.
/// </summary>
internal sealed class EventTypeIdCache
{
    private readonly ConcurrentDictionary<string, int> _tokenToId = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<int, string> _idToToken = new();

    public bool TryGetId(string token, out int id)
        => _tokenToId.TryGetValue(token, out id);

    public int? FindId(string token)
        => _tokenToId.TryGetValue(token, out var id) ? id : null;

    public bool TryGetToken(int id, out string token)
        => _idToToken.TryGetValue(id, out token!);

    public void Add(int id, string token)
    {
        _tokenToId[token] = id;
        _idToToken[id] = token;
    }

    public void ReplaceAll(IEnumerable<(int Id, string Token)> rows)
    {
        _tokenToId.Clear();
        _idToToken.Clear();
        foreach (var (id, token) in rows)
            Add(id, token);
    }
}
