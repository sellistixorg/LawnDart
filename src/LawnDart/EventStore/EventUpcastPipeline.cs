using System.Reflection;

namespace LawnDart.EventStore;

/// <summary>
/// Immutable upcast graph for one scoped catalog. Built by
/// <see cref="Materialize"/> / <c>WithUpcasters</c>.
/// </summary>
/// <remarks>
/// Each historical version in the catalog must reach current. There is no
/// downcast API. A missing hop fails at materialize (warmup), not at first
/// read of old data.
/// </remarks>
public sealed class EventUpcastPipeline
{
    private readonly EventTypeCatalog _catalog;
    private readonly Dictionary<Type, Invoker> _byFrom;

    private EventUpcastPipeline(EventTypeCatalog catalog, Dictionary<Type, Invoker> byFrom)
    {
        _catalog = catalog;
        _byFrom = byFrom;
    }

    /// <summary>
    /// Builds a pipeline from <paramref name="upcasterTypes"/> and fails if
    /// any historical catalog version cannot reach current.
    /// </summary>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="catalog"/> or <paramref name="upcasterTypes"/> is null.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// An upcaster is invalid (downcast, cross-family, duplicate source,
    /// type not in the catalog) or a historical version has no path to current.
    /// </exception>
    public static EventUpcastPipeline Materialize(
        EventTypeCatalog catalog,
        IEnumerable<Type> upcasterTypes)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(upcasterTypes);

        var byFrom = new Dictionary<Type, Invoker>();
        var seenImpl = new HashSet<Type>();
        foreach (var type in upcasterTypes)
        {
            if (type is null)
                throw new ArgumentException("Upcaster type list must not contain null entries.", nameof(upcasterTypes));
            if (!seenImpl.Add(type))
                continue;

            foreach (var (impl, to, from) in Discover(type))
                AddEdge(catalog, byFrom, impl, to, from);
        }

        EnsureComplete(catalog, byFrom);
        return new EventUpcastPipeline(catalog, byFrom);
    }

    /// <summary>
    /// Applies the chain from <paramref name="stored"/> to this process's
    /// current type for that family. Already-current instances are returned
    /// unchanged.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="stored"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="stored"/> is not in this pipeline's catalog.
    /// </exception>
    /// <exception cref="MissingEventUpcasterException">
    /// A hop is missing (should not happen after a successful <see cref="Materialize"/>).
    /// </exception>
    public IEvent UpcastToCurrent(IEvent stored)
    {
        ArgumentNullException.ThrowIfNull(stored);

        if (!_catalog.TryDescribe(stored.GetType(), out var token, out var fromVersion, out var current))
        {
            throw new InvalidOperationException(
                $"Event type '{stored.GetType().FullName}' is not in this process's catalog.");
        }

        if (stored.GetType() == current)
            return stored;

        var currentVersion = ReadVersion(current);
        IEvent cursor = stored;
        var hops = 0;
        while (cursor.GetType() != current)
        {
            if (hops++ > 64 || !_byFrom.TryGetValue(cursor.GetType(), out var step))
            {
                throw new MissingEventUpcasterException(token, fromVersion, currentVersion);
            }

            cursor = step.Invoke(cursor);
        }

        return cursor;
    }

    private static void AddEdge(
        EventTypeCatalog catalog,
        Dictionary<Type, Invoker> byFrom,
        Type impl,
        Type to,
        Type from)
    {
        if (!catalog.TryDescribe(from, out var fromToken, out var fromVersion, out _)
            || !catalog.TryDescribe(to, out var toToken, out var toVersion, out _))
        {
            throw new InvalidOperationException(
                $"Upcaster '{impl.FullName}' must map catalog types " +
                $"({from.FullName} → {to.FullName}).");
        }

        if (!string.Equals(fromToken, toToken, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Upcaster '{impl.FullName}' crosses event families " +
                $"'{fromToken}' → '{toToken}'. There is no downcast API.");
        }

        if (fromVersion >= toVersion)
        {
            throw new InvalidOperationException(
                $"Upcaster '{impl.FullName}' is a downcast " +
                $"(SchemaVersion {fromVersion} → {toVersion}) for family '{fromToken}'. " +
                "There is no downcast API.");
        }

        if (byFrom.ContainsKey(from))
        {
            throw new InvalidOperationException(
                $"Duplicate upcaster from '{from.FullName}' for family '{fromToken}'.");
        }

        if (Activator.CreateInstance(impl) is not { } instance)
        {
            throw new InvalidOperationException(
                $"Upcaster '{impl.FullName}' must have a public parameterless constructor.");
        }

        byFrom[from] = Invoker.Bind(instance, to, from);
    }

    private static void EnsureComplete(EventTypeCatalog catalog, Dictionary<Type, Invoker> byFrom)
    {
        foreach (var family in catalog.EnumerateFamilies())
        {
            foreach (var (version, type) in family.Members)
            {
                if (type == family.Current)
                    continue;

                if (!CanReach(type, family.Current, byFrom))
                {
                    throw new InvalidOperationException(
                        $"No upcaster chain from SchemaVersion {version} to {family.CurrentVersion} " +
                        $"for family '{family.Token}'. Register IEventUpcaster hops with WithUpcasters. " +
                        "Warmup is the runtime authority.");
                }
            }
        }
    }

    private static bool CanReach(Type from, Type current, Dictionary<Type, Invoker> byFrom)
    {
        var seen = new HashSet<Type>();
        var cursor = from;
        while (cursor != current)
        {
            if (!seen.Add(cursor) || !byFrom.TryGetValue(cursor, out var step))
                return false;
            cursor = step.To;
        }

        return true;
    }

    private static IEnumerable<(Type Impl, Type To, Type From)> Discover(Type type)
    {
        if (!type.IsClass || type.IsAbstract || type.IsGenericTypeDefinition)
        {
            throw new InvalidOperationException(
                $"Upcaster '{type.FullName}' must be a concrete class.");
        }

        var found = false;
        foreach (var iface in type.GetInterfaces())
        {
            if (!iface.IsGenericType || iface.GetGenericTypeDefinition() != typeof(IEventUpcaster<,>))
                continue;

            var args = iface.GetGenericArguments();
            found = true;
            yield return (type, args[0], args[1]);
        }

        if (!found)
        {
            throw new InvalidOperationException(
                $"Type '{type.FullName}' does not implement IEventUpcaster<TTo, TFrom>.");
        }
    }

    private static int ReadVersion(Type type)
    {
        var attr = type.GetCustomAttribute<EventTypeNameAttribute>(inherit: false);
        return attr is { Version: >= 1 } ? attr.Version : 1;
    }

    private sealed class Invoker
    {
        private readonly Func<IEvent, IEvent> _invoke;

        private Invoker(Type to, Func<IEvent, IEvent> invoke)
        {
            To = to;
            _invoke = invoke;
        }

        public Type To { get; }

        public IEvent Invoke(IEvent source) => _invoke(source);

        public static Invoker Bind(object instance, Type to, Type from)
        {
            var closed = typeof(Bound<,>).MakeGenericType(to, from);
            var bound = (IBound)Activator.CreateInstance(closed, instance)!;
            return new Invoker(to, bound.Upcast);
        }

        private interface IBound
        {
            IEvent Upcast(IEvent source);
        }

        private sealed class Bound<TTo, TFrom> : IBound
            where TTo : class, IEvent
            where TFrom : class, IEvent
        {
            private readonly IEventUpcaster<TTo, TFrom> _inner;

            public Bound(object instance)
            {
                _inner = (IEventUpcaster<TTo, TFrom>)instance;
            }

            public IEvent Upcast(IEvent source) => _inner.Upcast((TFrom)source);
        }
    }
}
