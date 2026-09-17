using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace LawnDart.EventStore;

/// <summary>
/// Immutable event-type catalog: family token plus <c>SchemaVersion</c> to CLR type.
/// </summary>
/// <remarks>
/// Call <see cref="Materialize"/> at context start and register that instance
/// with <c>WithEventTypes</c>. The catalog is per bounded context. There is no
/// process-wide resolver, no Shared instance, and no FullName / simple-name /
/// AssemblyQualifiedName read alias. An unknown token fails closed.
/// </remarks>
public sealed class EventTypeCatalog : IEventTypeCatalog
{
    private readonly CatalogMaps _maps;

    private EventTypeCatalog(CatalogMaps maps)
    {
        _maps = maps;
    }

    /// <summary>
    /// Builds a scoped immutable catalog from <paramref name="types"/>.
    /// </summary>
    /// <remarks>
    /// One-arg <c>[EventTypeName("token")]</c> is version 1 and implicitly
    /// current when it is the only type for that family. A family with two
    /// or more types needs exactly one <c>current: true</c>. Duplicate
    /// <c>(token, version)</c> or two current types fail closed.
    /// </remarks>
    public static EventTypeCatalog Materialize(IEnumerable<Type> types)
        => new(CatalogMaps.Build(types));

    /// <summary>
    /// Returns the declared family token when <paramref name="type"/> has
    /// <see cref="EventTypeNameAttribute"/>; otherwise <c>null</c>.
    /// Does not throw and does not use a FullName fallback.
    /// </summary>
    public static string? TryGetDeclaredName(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        var attr = type.GetCustomAttribute<EventTypeNameAttribute>(inherit: false);
        return string.IsNullOrWhiteSpace(attr?.Name) ? null : attr.Name;
    }

    /// <inheritdoc />
    public string GetName(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        if (_maps.TypeToToken.TryGetValue(type, out var token))
            return token;

        return TryGetDeclaredName(type)
            ?? throw new InvalidOperationException(
                $"Event type '{type.FullName}' must declare [EventTypeName(\"kebab-token\")]. " +
                "CLR FullName is not stored.");
    }

    /// <inheritdoc />
    public bool TryResolveType(string storedName, [NotNullWhen(true)] out Type? type)
    {
        if (string.IsNullOrWhiteSpace(storedName))
        {
            type = null;
            return false;
        }

        return _maps.CurrentByToken.TryGetValue(storedName, out type);
    }

    /// <inheritdoc />
    public bool TryResolveType(string storedName, int schemaVersion, [NotNullWhen(true)] out Type? type)
    {
        if (string.IsNullOrWhiteSpace(storedName))
        {
            type = null;
            return false;
        }

        var version = schemaVersion <= 0 ? 1 : schemaVersion;
        return _maps.TokenVersionToType.TryGetValue((storedName, version), out type);
    }

    /// <summary>
    /// Families from a <see cref="Materialize"/> catalog.
    /// </summary>
    internal IEnumerable<FamilySnapshot> EnumerateFamilies()
    {
        foreach (var token in _maps.FamilyTokens)
        {
            var current = _maps.CurrentByToken[token];
            var members = new List<(int Version, Type Type)>();
            foreach (var pair in _maps.TokenVersionToType)
            {
                if (string.Equals(pair.Key.Token, token, StringComparison.Ordinal))
                    members.Add((pair.Key.Version, pair.Value));
            }

            members.Sort((a, b) => a.Version.CompareTo(b.Version));
            var currentVersion = _maps.TypeToVersion[current];
            yield return new FamilySnapshot(token, current, currentVersion, members);
        }
    }

    internal bool TryDescribe(Type type, out string token, out int version, out Type current)
    {
        token = null!;
        version = 0;
        current = null!;
        if (type is null)
            return false;

        if (!_maps.TypeToToken.TryGetValue(type, out token!))
            return false;

        version = _maps.TypeToVersion[type];
        current = _maps.CurrentByToken[token];
        return true;
    }

    internal static void ValidateCatalogType(Type type)
    {
        if (!type.IsClass || type.IsAbstract || type.IsGenericTypeDefinition)
        {
            throw new InvalidOperationException(
                $"Event catalog type '{type.FullName}' must be a concrete class.");
        }

        if (!typeof(IEvent).IsAssignableFrom(type) || typeof(IRawEvent).IsAssignableFrom(type))
        {
            throw new InvalidOperationException(
                $"Event catalog type '{type.FullName}' must be a concrete {nameof(IEvent)} " +
                $"(not {nameof(IRawEvent)}).");
        }

        if (TryGetDeclaredName(type) is null)
        {
            throw new InvalidOperationException(
                $"Event type '{type.FullName}' must declare [EventTypeName(\"kebab-token\")].");
        }
    }

    internal readonly record struct FamilySnapshot(
        string Token,
        Type Current,
        int CurrentVersion,
        IReadOnlyList<(int Version, Type Type)> Members);

    internal sealed class CatalogMaps
    {
        public Dictionary<Type, string> TypeToToken { get; } = new();
        public Dictionary<Type, int> TypeToVersion { get; } = new();
        public Dictionary<(string Token, int Version), Type> TokenVersionToType { get; } = new();
        public Dictionary<string, Type> CurrentByToken { get; } = new(StringComparer.Ordinal);
        public HashSet<string> FamilyTokens { get; } = new(StringComparer.Ordinal);

        public static CatalogMaps Build(IEnumerable<Type> types)
        {
            ArgumentNullException.ThrowIfNull(types);

            var entries = new List<Registration>();
            var seenTypes = new HashSet<Type>();
            foreach (var type in types)
            {
                if (type is null)
                    throw new ArgumentException("Event type list must not contain null entries.", nameof(types));
                if (!seenTypes.Add(type))
                    continue;

                ValidateCatalogType(type);
                var attr = type.GetCustomAttribute<EventTypeNameAttribute>(inherit: false)
                    ?? throw new InvalidOperationException(
                        $"Event type '{type.FullName}' must declare [EventTypeName(\"kebab-token\")].");

                entries.Add(new Registration(type, attr.Name, attr.Version, attr.Current));
            }

            var seenVersion = new Dictionary<(string Token, int Version), Type>();
            foreach (var entry in entries)
            {
                var key = (entry.Token, entry.Version);
                if (seenVersion.TryGetValue(key, out var other) && other != entry.Type)
                {
                    throw new InvalidOperationException(
                        $"Duplicate event type token '{entry.Token}' version {entry.Version} " +
                        $"on '{entry.Type.FullName}' and '{other.FullName}'.");
                }

                seenVersion[key] = entry.Type;
            }

            var maps = new CatalogMaps();
            foreach (var family in entries.GroupBy(e => e.Token, StringComparer.Ordinal))
            {
                var members = family.ToList();
                Type currentType;
                if (members.Count == 1)
                {
                    currentType = members[0].Type;
                }
                else
                {
                    var currents = members.Where(m => m.Current).ToList();
                    if (currents.Count == 0)
                    {
                        throw new InvalidOperationException(
                            $"Event family '{family.Key}' has {members.Count} types and no " +
                            "[EventTypeName(..., current: true)].");
                    }

                    if (currents.Count > 1)
                    {
                        var names = string.Join(", ", currents.Select(c => $"'{c.Type.FullName}'"));
                        throw new InvalidOperationException(
                            $"Duplicate current for event type token '{family.Key}' on {names}.");
                    }

                    currentType = currents[0].Type;
                }

                maps.FamilyTokens.Add(family.Key);
                maps.CurrentByToken[family.Key] = currentType;

                foreach (var member in members)
                {
                    maps.TypeToToken[member.Type] = member.Token;
                    maps.TypeToVersion[member.Type] = member.Version;
                    maps.TokenVersionToType[(member.Token, member.Version)] = member.Type;
                }
            }

            return maps;
        }

        private readonly record struct Registration(Type Type, string Token, int Version, bool Current);
    }
}
