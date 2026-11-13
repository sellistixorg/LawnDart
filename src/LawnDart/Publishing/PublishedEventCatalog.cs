using System.Diagnostics.CodeAnalysis;
using LawnDart.EventStore;

namespace LawnDart.Publishing;

/// <summary>
/// Every published event type in the process: the union of each context's
/// <see cref="PublishedEventMap.PublishedTypes"/>. Registered once, not keyed.
/// </summary>
/// <remarks>
/// An outbox publisher reads notice rows back into .NET objects through this catalog,
/// for example with an <see cref="EventSession"/> built from it and
/// <see cref="UpcastPipeline"/>. Two contexts that publish the same event name with
/// different .NET types fail when the catalog is built.
/// </remarks>
public sealed class PublishedEventCatalog : IEventTypeCatalog
{
    private readonly EventTypeCatalog _catalog;

    private PublishedEventCatalog(
        EventTypeCatalog catalog,
        IReadOnlyCollection<Type> publishedTypes,
        EventUpcastPipeline? upcastPipeline)
    {
        _catalog = catalog;
        PublishedTypes = publishedTypes;
        UpcastPipeline = upcastPipeline;
    }

    /// <summary>Current published .NET types across every context.</summary>
    public IReadOnlyCollection<Type> PublishedTypes { get; }

    /// <summary>Upcast chain for published events, or <c>null</c> when no context declares one.</summary>
    public EventUpcastPipeline? UpcastPipeline { get; }

    /// <summary>
    /// Builds the process catalog from every context's map.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Two contexts publish one event name and schema version with different .NET types.
    /// </exception>
    public static PublishedEventCatalog Create(IEnumerable<PublishedEventMap> maps)
    {
        ArgumentNullException.ThrowIfNull(maps);

        var byName = new Dictionary<(string Name, int Version), (Type Type, string Context)>();
        var current = new Dictionary<string, (Type Type, string Context)>(StringComparer.Ordinal);
        var catalogTypes = new List<Type>();
        var upcasterTypes = new List<Type>();
        var published = new List<Type>();

        foreach (var map in maps)
        {
            ArgumentNullException.ThrowIfNull(map);

            foreach (var type in map.CatalogTypes)
            {
                var attr = (EventTypeNameAttribute)Attribute.GetCustomAttribute(type, typeof(EventTypeNameAttribute), inherit: false)!;
                var version = attr.Version >= 1 ? attr.Version : 1;
                if (byName.TryGetValue((attr.Name, version), out var seen) && seen.Type != type)
                    throw Conflict(attr.Name, seen, (type, map.ContextName));

                byName[(attr.Name, version)] = (type, map.ContextName);
                if (!catalogTypes.Contains(type))
                    catalogTypes.Add(type);
            }

            foreach (var type in map.PublishedTypes)
            {
                var name = EventTypeCatalog.TryGetDeclaredName(type)!;
                if (current.TryGetValue(name, out var seen) && seen.Type != type)
                    throw Conflict(name, seen, (type, map.ContextName));

                current[name] = (type, map.ContextName);
                if (!published.Contains(type))
                    published.Add(type);
            }

            foreach (var type in map.UpcasterTypes)
            {
                if (!upcasterTypes.Contains(type))
                    upcasterTypes.Add(type);
            }
        }

        var catalog = EventTypeCatalog.Materialize(catalogTypes);
        var pipeline = upcasterTypes.Count == 0
            ? null
            : EventUpcastPipeline.Materialize(catalog, upcasterTypes);
        return new PublishedEventCatalog(catalog, published.AsReadOnly(), pipeline);
    }

    /// <inheritdoc />
    public string GetName(Type type) => _catalog.GetName(type);

    /// <inheritdoc />
    public bool TryResolveType(string storedName, [NotNullWhen(true)] out Type? type)
        => _catalog.TryResolveType(storedName, out type);

    /// <inheritdoc />
    public bool TryResolveType(string storedName, int schemaVersion, [NotNullWhen(true)] out Type? type)
        => _catalog.TryResolveType(storedName, schemaVersion, out type);

    private static InvalidOperationException Conflict(
        string name,
        (Type Type, string Context) first,
        (Type Type, string Context) second)
        => new(
            $"Published event name '{name}' is used by context '{first.Context}' for '{first.Type.FullName}' " +
            $"and by context '{second.Context}' for '{second.Type.FullName}'. " +
            "Each published event name maps to one .NET type in a process. Prefix notice names with the owner, " +
            "for example 'membership.member-joined'.");
}

/// <summary>
/// Collects every context's <see cref="PublishedEventMap"/> in one service collection,
/// so the process catalog can be checked as each context is added.
/// </summary>
internal sealed class PublishedEventRegistry
{
    private readonly List<PublishedEventMap> _maps = [];

    public IReadOnlyList<PublishedEventMap> Maps => _maps;

    /// <summary>Adds <paramref name="map"/> after checking it against the maps already added.</summary>
    public void Add(PublishedEventMap map)
    {
        _ = PublishedEventCatalog.Create([.. _maps, map]);
        _maps.Add(map);
    }
}
