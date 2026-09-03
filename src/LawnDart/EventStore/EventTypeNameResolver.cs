using System.Collections.Concurrent;
using System.Reflection;

namespace LawnDart.EventStore;

/// <summary>
/// Resolves the canonical on-disk name for a CLR event type.
/// </summary>
/// <remarks>
/// <para>
/// Resolution order:
/// <list type="number">
///   <item><description>
///     If the type carries an <see cref="EventTypeNameAttribute"/>, its <c>Name</c> value is used.
///   </description></item>
///   <item><description>
///     Otherwise <c>Type.FullName</c> is used (namespace-qualified, e.g.
///     <c>My.Domain.Events.OrderPlaced</c>).
///   </description></item>
///   <item><description>
///     If <c>FullName</c> is <c>null</c> (anonymous types only), <c>Type.Name</c> is used as a
///     last resort.
///   </description></item>
/// </list>
/// </para>
/// <para>
/// Results are stored in a <see cref="ConcurrentDictionary{TKey,TValue}"/> keyed by <see cref="Type"/>
/// reference, so reflection is performed at most once per type per application lifetime.
/// Call <see cref="Warmup"/> at startup with all known event types to ensure the cache is
/// fully populated before the first event write or read — guaranteeing zero reflection on
/// the hot path.
/// </para>
/// </remarks>
public static class EventTypeNameResolver
{
    private static readonly ConcurrentDictionary<Type, string> _cache = new();

    /// <summary>
    /// Returns the canonical on-disk name for <paramref name="type"/>.
    /// </summary>
    /// <remarks>
    /// On first call for a given type, performs one reflection lookup to check for
    /// <see cref="EventTypeNameAttribute"/>.  All subsequent calls for the same type are
    /// satisfied from the internal cache with no reflection.
    /// </remarks>
    /// <param name="type">The CLR event type to resolve.</param>
    /// <returns>The stable name to store in the event log.</returns>
    public static string GetName(Type type)
        => _cache.GetOrAdd(type, static t =>
        {
            var attr = t.GetCustomAttribute<EventTypeNameAttribute>(inherit: false);
            return attr?.Name ?? t.FullName ?? t.Name;
        });

    /// <summary>
    /// Pre-populates the cache for all <paramref name="types"/> so that every subsequent
    /// <see cref="GetName"/> call for a registered type is a pure cache hit with no reflection.
    /// </summary>
    /// <remarks>
    /// Call this once during application startup — typically alongside the
    /// <c>AddBoundedContext</c> registration — before any events are written or read.
    /// </remarks>
    /// <param name="types">The complete set of event types that will be used at runtime.</param>
    public static void Warmup(IEnumerable<Type> types)
    {
        foreach (var t in types)
            GetName(t);
    }

    /// <summary>
    /// Removes all cached entries. Intended for use in unit tests only.
    /// </summary>
    internal static void ResetForTesting() => _cache.Clear();
}
