using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace LawnDart.EventStore;

/// <summary>
/// Per-store catalog used when a session is constructed without
/// <see cref="EventTypeCatalog.Materialize"/>. GetName records the type;
/// resolve only returns tokens this instance has seen. Not process-wide.
/// </summary>
internal sealed class WriteThroughEventTypeCatalog : IEventTypeCatalog
{
    private readonly ConcurrentDictionary<Type, string> _typeToToken = new();
    private readonly ConcurrentDictionary<string, Type> _currentByToken = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<(string Token, int Version), Type> _tokenVersionToType = new();

    public string GetName(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return _typeToToken.GetOrAdd(type, static (t, catalog) =>
        {
            var token = EventTypeCatalog.TryGetDeclaredName(t)
                ?? throw new InvalidOperationException(
                    $"Event type '{t.FullName}' must declare [EventTypeName(\"kebab-token\")]. " +
                    "CLR FullName is not stored.");
            var version = ReadVersion(t);
            catalog._tokenVersionToType.TryAdd((token, version), t);
            catalog._currentByToken.TryAdd(token, t);
            return token;
        }, this);
    }

    public bool TryResolveType(string storedName, [NotNullWhen(true)] out Type? type)
    {
        if (string.IsNullOrWhiteSpace(storedName))
        {
            type = null;
            return false;
        }

        return _currentByToken.TryGetValue(storedName, out type);
    }

    public bool TryResolveType(string storedName, int schemaVersion, [NotNullWhen(true)] out Type? type)
    {
        if (string.IsNullOrWhiteSpace(storedName))
        {
            type = null;
            return false;
        }

        var version = schemaVersion <= 0 ? 1 : schemaVersion;
        return _tokenVersionToType.TryGetValue((storedName, version), out type);
    }

    private static int ReadVersion(Type type)
    {
        var attr = type.GetCustomAttribute<EventTypeNameAttribute>(inherit: false);
        return attr is { Version: >= 1 } ? attr.Version : 1;
    }
}
