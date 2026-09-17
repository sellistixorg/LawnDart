using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace LawnDart.EventStore;

/// <summary>
/// Process-wide compatibility wrapper for catalog tokens.
/// </summary>
/// <remarks>
/// Prefer <see cref="EventTypeCatalog.Materialize"/> registered per bounded
/// context. <see cref="Warmup"/> still fills these process-wide maps so hosts
/// that skip <c>WithEventTypes</c> can resolve types. <see cref="GetName"/>
/// returns the family token (not <c>token.v2</c>).
/// </remarks>
public static class EventTypeNameResolver
{
    private static readonly ConcurrentDictionary<Type, string> TypeToToken = new();
    private static readonly ConcurrentDictionary<string, Type> NameToType = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<(string Token, int Version), Type> TokenVersionToType = new();
    private static readonly ConcurrentDictionary<string, Type> CurrentByToken = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, byte> FamilyTokens = new(StringComparer.Ordinal);

    /// <summary>
    /// Returns the catalog token to store for <paramref name="type"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The type does not declare <see cref="EventTypeNameAttribute"/>.
    /// </exception>
    public static string GetName(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return TypeToToken.GetOrAdd(type, static t =>
        {
            var token = RequireToken(t);
            var version = ReadVersion(t);
            RegisterReadAlias(token, t);
            TokenVersionToType.TryAdd((token, version), t);
            CurrentByToken.TryAdd(token, t);
            FamilyTokens.TryAdd(token, 0);
            return token;
        });
    }

    /// <summary>
    /// Registers catalog types, fails on missing tokens, duplicate
    /// <c>(token, version)</c>, or L21 current-rule violations, and records
    /// FullName / simple-name read aliases.
    /// </summary>
    public static void Warmup(IEnumerable<Type> types)
    {
        EventTypeCatalog.Materialize(types).InstallIntoProcessResolver();
    }

    /// <summary>
    /// Resolves a stored type name (catalog token, FullName, or simple name) to a CLR type.
    /// After the dictionary miss, inherited SQL aliases scan AppDomain FullName /
    /// AssemblyQualifiedName and undeclared <see cref="EventTypeNameAttribute"/> tokens.
    /// Token-only resolve returns the family's current type.
    /// </summary>
    public static bool TryResolveType(string storedName, [NotNullWhen(true)] out Type? type)
    {
        if (string.IsNullOrWhiteSpace(storedName))
        {
            type = null;
            return false;
        }

        if (CurrentByToken.TryGetValue(storedName, out type))
            return true;

        if (NameToType.TryGetValue(storedName, out type))
            return true;

        type = TryResolveLegacyStoredName(storedName);
        if (type is null)
            return false;

        RegisterReadAlias(storedName, type);
        return true;
    }

    /// <summary>
    /// Resolves a stored family token (or a read alias) at
    /// <paramref name="schemaVersion"/>. Missing or zero version treats as 1.
    /// A known family with no CLR type for that version fails closed (no
    /// AppDomain fallback).
    /// </summary>
    public static bool TryResolveType(string storedName, int schemaVersion, [NotNullWhen(true)] out Type? type)
    {
        if (string.IsNullOrWhiteSpace(storedName))
        {
            type = null;
            return false;
        }

        var version = schemaVersion <= 0 ? 1 : schemaVersion;
        if (TokenVersionToType.TryGetValue((storedName, version), out type))
            return true;

        if (FamilyTokens.ContainsKey(storedName) || CurrentByToken.ContainsKey(storedName))
        {
            type = null;
            return false;
        }

        if (NameToType.TryGetValue(storedName, out type))
            return true;

        type = TryResolveLegacyStoredName(storedName);
        if (type is null)
            return false;

        RegisterReadAlias(storedName, type);
        return true;
    }

    /// <summary>
    /// Returns the declared token when <paramref name="type"/> has
    /// <see cref="EventTypeNameAttribute"/>; otherwise <c>null</c>.
    /// Does not throw and does not use a FullName fallback.
    /// </summary>
    public static string? TryGetDeclaredName(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        var attr = type.GetCustomAttribute<EventTypeNameAttribute>(inherit: false);
        return string.IsNullOrWhiteSpace(attr?.Name) ? null : attr.Name;
    }

    internal static void ResetForTesting()
    {
        TypeToToken.Clear();
        NameToType.Clear();
        TokenVersionToType.Clear();
        CurrentByToken.Clear();
        FamilyTokens.Clear();
    }

    internal static void Install(EventTypeCatalog.CatalogMaps maps)
    {
        foreach (var pair in maps.TypeToToken)
            TypeToToken[pair.Key] = pair.Value;

        foreach (var pair in maps.TokenVersionToType)
            TokenVersionToType[pair.Key] = pair.Value;

        foreach (var pair in maps.CurrentByToken)
        {
            CurrentByToken[pair.Key] = pair.Value;
            FamilyTokens[pair.Key] = 0;
            NameToType[pair.Key] = pair.Value;
        }

        foreach (var pair in maps.Aliases)
            RegisterReadAlias(pair.Key, pair.Value);
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

    private static string RequireToken(Type type)
    {
        var token = TryGetDeclaredName(type);
        if (token is null)
        {
            throw new InvalidOperationException(
                $"Event type '{type.FullName}' must declare [EventTypeName(\"kebab-token\")]. " +
                "CLR FullName is not stored.");
        }

        return token;
    }

    private static int ReadVersion(Type type)
    {
        var attr = type.GetCustomAttribute<EventTypeNameAttribute>(inherit: false);
        return attr is { Version: >= 1 } ? attr.Version : 1;
    }

    private static void RegisterReadAlias(string? name, Type type)
    {
        if (string.IsNullOrWhiteSpace(name))
            return;

        NameToType.AddOrUpdate(name, type, (_, existing) => existing);
    }

    /// <summary>
    /// Inherited SQL read aliases for older rows stored as CLR FullName,
    /// AssemblyQualifiedName, or a catalog token that has not been Warmup'd yet.
    /// </summary>
    private static Type? TryResolveLegacyStoredName(string typeName)
    {
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                var t = assembly.GetType(typeName, throwOnError: false);
                if (t != null) return t;
            }
            catch
            {
                // skip unloadable assemblies
            }
        }

        var commaIndex = typeName.IndexOf(',');
        if (commaIndex > 0)
        {
            var fullName = typeName[..commaIndex];

            var directType = Type.GetType(typeName, throwOnError: false);
            if (directType != null) return directType;

            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                try
                {
                    var t = assembly.GetType(fullName, throwOnError: false);
                    if (t != null) return t;

                    t = Array.Find(assembly.GetTypes(),
                        x => x.FullName == fullName
                             || x.AssemblyQualifiedName?.StartsWith(fullName + ",", StringComparison.Ordinal) == true);
                    if (t != null) return t;
                }
                catch (ReflectionTypeLoadException ex)
                {
                    if (ex.Types == null) continue;
                    foreach (var loaded in ex.Types)
                    {
                        if (loaded?.FullName == fullName) return loaded;
                    }
                }
                catch
                {
                    // skip unloadable assemblies
                }
            }
        }

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                foreach (var candidate in assembly.GetTypes())
                {
                    if (!IsCatalogEventType(candidate))
                        continue;

                    if (string.Equals(TryGetDeclaredName(candidate), typeName, StringComparison.Ordinal))
                        return candidate;
                }
            }
            catch (ReflectionTypeLoadException ex)
            {
                if (ex.Types == null) continue;
                foreach (var candidate in ex.Types)
                {
                    if (candidate is null || !IsCatalogEventType(candidate))
                        continue;

                    if (string.Equals(TryGetDeclaredName(candidate), typeName, StringComparison.Ordinal))
                        return candidate;
                }
            }
            catch
            {
                // skip unloadable assemblies
            }
        }

        return null;
    }

    private static bool IsCatalogEventType(Type type)
        => typeof(IEvent).IsAssignableFrom(type) && !type.IsAbstract && !type.IsInterface;
}
