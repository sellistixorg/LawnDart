using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace LawnDart.EventStore;

/// <summary>
/// Resolves the canonical on-disk name for a CLR event type.
/// </summary>
/// <remarks>
/// <para>
/// Writes use a stable catalog token from <see cref="EventTypeNameAttribute"/>.
/// CLR <c>FullName</c> is never stored. Call <see cref="Warmup"/> or
/// <c>WithEventTypes</c> at startup so duplicate tokens fail closed and read
/// aliases (<c>FullName</c>, simple name) are registered for older rows.
/// </para>
/// </remarks>
public static class EventTypeNameResolver
{
    private static readonly ConcurrentDictionary<Type, string> TypeToToken = new();
    private static readonly ConcurrentDictionary<string, Type> NameToType = new(StringComparer.Ordinal);

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
            RegisterReadAlias(token, t);
            return token;
        });
    }

    /// <summary>
    /// Registers catalog types, fails on missing tokens or duplicates in
    /// <paramref name="types"/>, and records FullName / simple-name read aliases.
    /// </summary>
    public static void Warmup(IEnumerable<Type> types)
    {
        ArgumentNullException.ThrowIfNull(types);

        var seen = new Dictionary<string, Type>(StringComparer.Ordinal);
        foreach (var type in types)
        {
            if (type is null)
                throw new ArgumentException("Event type list must not contain null entries.", nameof(types));

            ValidateCatalogType(type);
            var token = GetName(type);
            if (seen.TryGetValue(token, out var other) && other != type)
            {
                throw new InvalidOperationException(
                    $"Duplicate event type token '{token}' on '{type.FullName}' and '{other.FullName}'.");
            }

            seen[token] = type;
            RegisterReadAlias(type.FullName, type);
            RegisterReadAlias(type.Name, type);
        }
    }

    /// <summary>
    /// Resolves a stored type name (catalog token, FullName, or simple name) to a CLR type.
    /// After the dictionary miss, inherited SQL aliases scan AppDomain FullName /
    /// AssemblyQualifiedName and undeclared <see cref="EventTypeNameAttribute"/> tokens.
    /// </summary>
    public static bool TryResolveType(string storedName, [NotNullWhen(true)] out Type? type)
    {
        if (string.IsNullOrWhiteSpace(storedName))
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

    private static void ValidateCatalogType(Type type)
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
