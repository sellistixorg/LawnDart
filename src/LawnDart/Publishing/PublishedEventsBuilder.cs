using System.Reflection;
using System.Runtime.CompilerServices;
using LawnDart.EventStore;

namespace LawnDart.Publishing;

/// <summary>
/// Declares which events one bounded context publishes, and in what shape.
/// Used inside <see cref="BoundedContextExtensions.PublishEvents"/>.
/// </summary>
/// <remarks>
/// <para>
/// An event is published only when it is declared here. <see cref="Publish{TEvent}"/>
/// passes the event through as is, which suits events that already live in a
/// contracts project. <see cref="Translate{TInternal, TPublic}(Func{TInternal, TPublic})"/>
/// turns an internal event into a separate integration event, which is the normal case.
/// </para>
/// <para>
/// Each internal event type is declared at most once, so one internal event maps to at
/// most one published event. Published types need <see cref="EventTypeNameAttribute"/>.
/// </para>
/// </remarks>
public sealed class PublishedEventsBuilder
{
    private readonly string _contextName;
    private readonly Dictionary<Type, PublishedEventMapping> _mappings = new();
    private readonly List<PublishedEventMapping> _ordered = [];
    private Assembly[]? _upcasterAssemblies;

    internal PublishedEventsBuilder(string contextName)
    {
        _contextName = contextName;
    }

    /// <summary>
    /// Publishes <typeparamref name="TEvent"/> as is. The internal event becomes the
    /// public contract, so use this only for events that already live in a contracts project.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// <typeparamref name="TEvent"/> has no <see cref="EventTypeNameAttribute"/>, or it is already declared.
    /// </exception>
    public PublishedEventsBuilder Publish<TEvent>()
        where TEvent : class, IEvent
    {
        Add(new PublishedEventMapping(
            typeof(TEvent),
            typeof(TEvent),
            static (e, _) => e,
            $"Publish<{typeof(TEvent).Name}>()"));
        return this;
    }

    /// <summary>
    /// Publishes <typeparamref name="TPublic"/> in place of each <typeparamref name="TInternal"/>.
    /// Return <c>null</c> from <paramref name="translate"/> to publish nothing for that event.
    /// </summary>
    /// <remarks>
    /// The translation is pure and synchronous and runs inside the append. It may read
    /// only the event. By convention the notice id is the internal event's id.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// <typeparamref name="TPublic"/> has no <see cref="EventTypeNameAttribute"/>, or
    /// <typeparamref name="TInternal"/> is already declared.
    /// </exception>
    public PublishedEventsBuilder Translate<TInternal, TPublic>(Func<TInternal, TPublic?> translate)
        where TInternal : class, IEvent
        where TPublic : class, IEvent
    {
        ArgumentNullException.ThrowIfNull(translate);
        Add(new PublishedEventMapping(
            typeof(TInternal),
            typeof(TPublic),
            (e, _) => translate((TInternal)e),
            $"Translate<{typeof(TInternal).Name}, {typeof(TPublic).Name}>(...)"));
        return this;
    }

    /// <summary>
    /// Publishes <typeparamref name="TPublic"/> in place of each <typeparamref name="TInternal"/>,
    /// with <see cref="PublishContext"/> for the source id, tags, tenant and correlation.
    /// Return <c>null</c> from <paramref name="translate"/> to publish nothing for that event.
    /// </summary>
    /// <remarks>
    /// The translation is pure and synchronous and runs inside the append. It may read
    /// only the event and the context. By convention the notice id is
    /// <see cref="PublishContext.SourceEventId"/>.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// <typeparamref name="TPublic"/> has no <see cref="EventTypeNameAttribute"/>, or
    /// <typeparamref name="TInternal"/> is already declared.
    /// </exception>
    public PublishedEventsBuilder Translate<TInternal, TPublic>(Func<TInternal, PublishContext, TPublic?> translate)
        where TInternal : class, IEvent
        where TPublic : class, IEvent
    {
        ArgumentNullException.ThrowIfNull(translate);
        Add(new PublishedEventMapping(
            typeof(TInternal),
            typeof(TPublic),
            (e, ctx) => translate((TInternal)e, ctx),
            $"Translate<{typeof(TInternal).Name}, {typeof(TPublic).Name}>(...)"));
        return this;
    }

    /// <summary>
    /// Adds every <see cref="IEventTranslator{TInternal, TPublic}"/> that
    /// <typeparamref name="TTranslator"/> implements. One instance is created now with
    /// its public parameterless constructor.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// <typeparamref name="TTranslator"/> is not a concrete translator, has no public
    /// parameterless constructor, or declares an internal event that is already declared.
    /// </exception>
    public PublishedEventsBuilder AddTranslator<TTranslator>()
        where TTranslator : class
    {
        AddTranslatorType(typeof(TTranslator));
        return this;
    }

    /// <summary>
    /// Scans <typeparamref name="TMarker"/>'s assembly for concrete
    /// <see cref="IEventTranslator{TInternal, TPublic}"/> types and adds them.
    /// Prefer this overload. It survives renames.
    /// </summary>
    /// <exception cref="InvalidOperationException">The scan found no translators.</exception>
    public PublishedEventsBuilder AddTranslators<TMarker>()
        => AddTranslators(typeof(TMarker).Assembly);

    /// <summary>
    /// Scans assemblies for exported, concrete, non-generic
    /// <see cref="IEventTranslator{TInternal, TPublic}"/> types and adds them.
    /// With no assembly, the calling assembly is scanned.
    /// </summary>
    /// <exception cref="InvalidOperationException">The scan found no translators.</exception>
    [MethodImpl(MethodImplOptions.NoInlining)]
    public PublishedEventsBuilder AddTranslators(params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);
        var usedCallingAssemblyFallback = assemblies.Length == 0;
        if (usedCallingAssemblyFallback)
            assemblies = [Assembly.GetCallingAssembly()];

        var found = new List<Type>();
        foreach (var assembly in assemblies)
        {
            ArgumentNullException.ThrowIfNull(assembly);
            foreach (var type in assembly.GetExportedTypes())
            {
                if (!type.IsClass || type.IsAbstract || type.IsGenericTypeDefinition)
                    continue;
                if (TranslatorInterfaces(type).Count == 0)
                    continue;
                found.Add(type);
            }
        }

        if (found.Count == 0)
        {
            throw new InvalidOperationException(
                FormatZeroScanMessage("AddTranslators", "IEventTranslator<,>", assemblies, usedCallingAssemblyFallback));
        }

        foreach (var type in found)
            AddTranslatorType(type);
        return this;
    }

    /// <summary>
    /// Scans <typeparamref name="TMarker"/>'s assembly for <see cref="IEventUpcaster{TTo, TFrom}"/>
    /// types whose event name is published here, so an outbox row written with an older
    /// schema version can still be read back. Upcasters for internal events are ignored.
    /// </summary>
    /// <exception cref="InvalidOperationException">Upcasters were already added for this context.</exception>
    public PublishedEventsBuilder WithUpcasters<TMarker>()
        => WithUpcasters(typeof(TMarker).Assembly);

    /// <summary>
    /// Scans assemblies for <see cref="IEventUpcaster{TTo, TFrom}"/> types whose event name
    /// is published here. Older versions of a published event come from these upcasters,
    /// and each one must reach the current version. With no assembly, the calling
    /// assembly is scanned.
    /// </summary>
    /// <exception cref="InvalidOperationException">Upcasters were already added for this context.</exception>
    [MethodImpl(MethodImplOptions.NoInlining)]
    public PublishedEventsBuilder WithUpcasters(params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);
        if (_upcasterAssemblies is not null)
        {
            throw new InvalidOperationException(
                $"PublishEvents WithUpcasters has already been called for context '{_contextName}'.");
        }

        if (assemblies.Length == 0)
            assemblies = [Assembly.GetCallingAssembly()];
        foreach (var assembly in assemblies)
            ArgumentNullException.ThrowIfNull(assembly);

        _upcasterAssemblies = assemblies;
        return this;
    }

    /// <summary>
    /// Checks the declarations against the context's own catalog and builds the map.
    /// </summary>
    internal PublishedEventMap Build(EventTypeCatalog sourceCatalog)
    {
        ArgumentNullException.ThrowIfNull(sourceCatalog);

        if (_ordered.Count == 0)
            throw new InvalidOperationException(PublishedEventMap.NothingDeclaredMessage(_contextName, sourceCatalog));

        foreach (var mapping in _ordered)
        {
            if (!sourceCatalog.TryDescribe(mapping.Source, out var token, out _, out var current))
            {
                throw new InvalidOperationException(
                    $"PublishEvents for context '{_contextName}' declares '{mapping.Source.FullName}' in {mapping.Origin}, " +
                    "but that type is not in this context's WithEventTypes catalog. " +
                    "Only events this context appends can be published.");
            }

            if (current != mapping.Source)
            {
                throw new InvalidOperationException(
                    $"PublishEvents for context '{_contextName}' declares '{mapping.Source.FullName}' in {mapping.Origin}, " +
                    $"which is an older schema version of '{token}'. Declare the current type '{current.FullName}'.");
            }
        }

        var publishedNames = new Dictionary<string, Type>(StringComparer.Ordinal);
        foreach (var mapping in _ordered)
        {
            var name = EventTypeCatalog.TryGetDeclaredName(mapping.Target)!;
            if (publishedNames.TryGetValue(name, out var other) && other != mapping.Target)
            {
                throw new InvalidOperationException(
                    $"PublishEvents for context '{_contextName}' publishes the event name '{name}' " +
                    $"as both '{other.FullName}' and '{mapping.Target.FullName}'. One name maps to one .NET type.");
            }

            publishedNames[name] = mapping.Target;
        }

        var catalogTypes = new List<Type>(publishedNames.Values);
        var upcasterTypes = new List<Type>();
        if (_upcasterAssemblies is not null)
            CollectUpcasters(_upcasterAssemblies, publishedNames, catalogTypes, upcasterTypes);

        var catalog = EventTypeCatalog.Materialize(catalogTypes);
        foreach (var target in publishedNames.Values)
        {
            if (catalog.TryDescribe(target, out var token, out _, out var current) && current != target)
            {
                throw new InvalidOperationException(
                    $"PublishEvents for context '{_contextName}' publishes '{target.FullName}', which is an older " +
                    $"schema version of '{token}'. Publish the current type '{current.FullName}'.");
            }
        }

        var pipeline = upcasterTypes.Count == 0
            ? null
            : EventUpcastPipeline.Materialize(catalog, upcasterTypes);

        return new PublishedEventMap(
            _contextName,
            _mappings,
            [.. _ordered.Select(m => m.Source)],
            [.. publishedNames.Values],
            catalog,
            catalogTypes,
            upcasterTypes,
            pipeline);
    }

    private void AddTranslatorType(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        if (!type.IsClass || type.IsAbstract || type.IsGenericTypeDefinition)
        {
            throw new InvalidOperationException(
                $"Translator '{type.FullName}' must be a concrete, non-generic class.");
        }

        var interfaces = TranslatorInterfaces(type);
        if (interfaces.Count == 0)
        {
            throw new InvalidOperationException(
                $"Type '{type.FullName}' does not implement IEventTranslator<TInternal, TPublic>.");
        }

        if (type.GetConstructor(Type.EmptyTypes) is null)
        {
            throw new InvalidOperationException(
                $"Translator '{type.FullName}' must have a public parameterless constructor. " +
                "Translators are created once at startup and cannot take services.");
        }

        var instance = Activator.CreateInstance(type)!;
        foreach (var iface in interfaces)
        {
            var args = iface.GetGenericArguments();
            var bound = (ITranslatorBinding)Activator.CreateInstance(
                typeof(TranslatorBinding<,>).MakeGenericType(args[0], args[1]),
                instance)!;
            Add(new PublishedEventMapping(
                args[0],
                args[1],
                bound.Translate,
                $"translator '{type.FullName}'"));
        }
    }

    private void Add(PublishedEventMapping mapping)
    {
        RequireEventClass(mapping.Source, "Internal event");
        RequireEventClass(mapping.Target, "Published event");
        if (EventTypeCatalog.TryGetDeclaredName(mapping.Target) is null)
        {
            throw new InvalidOperationException(
                $"Published event type '{mapping.Target.FullName}' in {mapping.Origin} must declare " +
                "[EventTypeName(\"kebab-token\")]. Published events need a stable name.");
        }

        if (_mappings.TryGetValue(mapping.Source, out var existing))
        {
            throw new InvalidOperationException(
                $"PublishEvents for context '{_contextName}' declares '{mapping.Source.FullName}' twice " +
                $"({existing.Origin} and {mapping.Origin}). One internal event maps to at most one published event.");
        }

        _mappings.Add(mapping.Source, mapping);
        _ordered.Add(mapping);
    }

    private static void RequireEventClass(Type type, string role)
    {
        if (!type.IsClass || type.IsAbstract || type.IsGenericTypeDefinition
            || !typeof(IEvent).IsAssignableFrom(type) || typeof(IRawEvent).IsAssignableFrom(type))
        {
            throw new InvalidOperationException(
                $"{role} type '{type.FullName}' must be a concrete {nameof(IEvent)} class (not {nameof(IRawEvent)}).");
        }
    }

    private static void CollectUpcasters(
        Assembly[] assemblies,
        Dictionary<string, Type> publishedNames,
        List<Type> catalogTypes,
        List<Type> upcasterTypes)
    {
        foreach (var assembly in assemblies)
        {
            foreach (var type in assembly.GetExportedTypes())
            {
                if (!type.IsClass || type.IsAbstract || type.IsGenericTypeDefinition)
                    continue;

                var keep = false;
                foreach (var iface in type.GetInterfaces())
                {
                    if (!iface.IsGenericType || iface.GetGenericTypeDefinition() != typeof(IEventUpcaster<,>))
                        continue;

                    var args = iface.GetGenericArguments();
                    var toName = EventTypeCatalog.TryGetDeclaredName(args[0]);
                    if (toName is null || !publishedNames.ContainsKey(toName))
                        continue;

                    keep = true;
                    if (!catalogTypes.Contains(args[0]))
                        catalogTypes.Add(args[0]);
                    if (!catalogTypes.Contains(args[1]))
                        catalogTypes.Add(args[1]);
                }

                if (keep)
                    upcasterTypes.Add(type);
            }
        }
    }

    private static List<Type> TranslatorInterfaces(Type type)
    {
        var list = new List<Type>();
        foreach (var iface in type.GetInterfaces())
        {
            if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IEventTranslator<,>))
                list.Add(iface);
        }

        return list;
    }

    private static string FormatZeroScanMessage(
        string api,
        string kind,
        IReadOnlyList<Assembly> assemblies,
        bool usedCallingAssemblyFallback)
    {
        var names = string.Join(", ", assemblies.Select(a => $"'{a.GetName().Name}'"));
        var scanned = assemblies.Count == 1
            ? $"assembly {names}"
            : $"assemblies {names}";

        if (usedCallingAssemblyFallback)
        {
            return $"{api} scanned {scanned} (chosen by Assembly.GetCallingAssembly() because no assembly was passed) and found no {kind} implementations.";
        }

        return $"{api} scanned {scanned} and found no {kind} implementations.";
    }

    private interface ITranslatorBinding
    {
        IEvent? Translate(IEvent source, PublishContext context);
    }

    private sealed class TranslatorBinding<TInternal, TPublic> : ITranslatorBinding
        where TInternal : class, IEvent
        where TPublic : class, IEvent
    {
        private readonly IEventTranslator<TInternal, TPublic> _inner;

        public TranslatorBinding(object instance)
        {
            _inner = (IEventTranslator<TInternal, TPublic>)instance;
        }

        public IEvent? Translate(IEvent source, PublishContext context)
            => _inner.Translate((TInternal)source, context);
    }
}

/// <summary>One declared source type and how it becomes a published event.</summary>
internal sealed record PublishedEventMapping(
    Type Source,
    Type Target,
    Func<IEvent, PublishContext, IEvent?> Translate,
    string Origin);
