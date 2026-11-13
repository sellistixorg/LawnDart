using System.Diagnostics.CodeAnalysis;
using LawnDart.EventStore;

namespace LawnDart.Publishing;

/// <summary>
/// The events one bounded context publishes, and how each internal event becomes
/// its published shape. Immutable. Built at startup by
/// <see cref="BoundedContextExtensions.PublishEvents"/> and registered as a keyed
/// singleton under the context name.
/// </summary>
/// <remarks>
/// <para>
/// A store, relay or test outside LawnDart can resolve the map and call
/// <see cref="TryTranslate"/> to apply the same rule: an event that is not declared
/// is never published.
/// </para>
/// <para>
/// <see cref="TryTranslate"/> runs the declared translation, which is pure and
/// synchronous. It never touches a store or a service.
/// </para>
/// </remarks>
public sealed class PublishedEventMap
{
    private readonly Dictionary<Type, PublishedEventMapping> _mappings;

    internal PublishedEventMap(
        string contextName,
        Dictionary<Type, PublishedEventMapping> mappings,
        IReadOnlyCollection<Type> sourceTypes,
        IReadOnlyCollection<Type> publishedTypes,
        EventTypeCatalog catalog,
        IReadOnlyList<Type> catalogTypes,
        IReadOnlyList<Type> upcasterTypes,
        EventUpcastPipeline? upcastPipeline)
    {
        ContextName = contextName;
        _mappings = new Dictionary<Type, PublishedEventMapping>(mappings);
        SourceTypes = sourceTypes;
        PublishedTypes = publishedTypes;
        Catalog = catalog;
        CatalogTypes = catalogTypes;
        UpcasterTypes = upcasterTypes;
        UpcastPipeline = upcastPipeline;
    }

    /// <summary>The bounded context this map belongs to.</summary>
    public string ContextName { get; }

    /// <summary>Internal event types that are declared, in declaration order.</summary>
    public IReadOnlyCollection<Type> SourceTypes { get; }

    /// <summary>
    /// Current .NET types that go in outbox rows: pass-through types and translation targets.
    /// </summary>
    public IReadOnlyCollection<Type> PublishedTypes { get; }

    /// <summary>Catalog of the published types, including older versions added by upcasters.</summary>
    internal EventTypeCatalog Catalog { get; }

    /// <summary>Every type in <see cref="Catalog"/>.</summary>
    internal IReadOnlyList<Type> CatalogTypes { get; }

    /// <summary>Upcaster types for published events.</summary>
    internal IReadOnlyList<Type> UpcasterTypes { get; }

    /// <summary>Upcast chain for published events, or <c>null</c> when there is none.</summary>
    internal EventUpcastPipeline? UpcastPipeline { get; }

    /// <summary>
    /// Builds a map without a service collection, for a store, relay or test that
    /// has the context's catalog in hand. Runs the same startup checks as
    /// <see cref="BoundedContextExtensions.PublishEvents"/>.
    /// </summary>
    /// <param name="contextName">The bounded context name.</param>
    /// <param name="sourceCatalog">The context's own catalog from <c>WithEventTypes</c>.</param>
    /// <param name="configure">The declarations.</param>
    /// <exception cref="InvalidOperationException">A declaration fails a startup check, or nothing is declared.</exception>
    public static PublishedEventMap Create(
        string contextName,
        EventTypeCatalog sourceCatalog,
        Action<PublishedEventsBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(contextName);
        ArgumentNullException.ThrowIfNull(sourceCatalog);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new PublishedEventsBuilder(contextName);
        configure(builder);
        return builder.Build(sourceCatalog);
    }

    /// <summary>
    /// Runs the declared translation for <paramref name="source"/>.
    /// </summary>
    /// <param name="source">The internal event being appended.</param>
    /// <param name="context">What the translation may read besides the event.</param>
    /// <param name="notice">
    /// The event to publish: <paramref name="source"/> itself for a pass-through, or the
    /// integration event for a translation. <c>null</c> when nothing is published.
    /// </param>
    /// <returns>
    /// <c>true</c> when an event is published; <c>false</c> when the type is not declared
    /// or its translation returned <c>null</c>.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The translation returned a type other than the one it declared.
    /// </exception>
    public bool TryTranslate(
        IEvent source,
        in PublishContext context,
        [NotNullWhen(true)] out IEvent? notice)
    {
        ArgumentNullException.ThrowIfNull(source);

        notice = null;
        if (!_mappings.TryGetValue(source.GetType(), out var mapping))
            return false;

        var result = mapping.Translate(source, context);
        if (result is null)
            return false;

        if (result.GetType() != mapping.Target)
        {
            throw new InvalidOperationException(
                $"The translation for '{mapping.Source.FullName}' in context '{ContextName}' returned " +
                $"'{result.GetType().FullName}', but it declares '{mapping.Target.FullName}'.");
        }

        notice = result;
        return true;
    }

    /// <summary>
    /// Message for a context that turns the outbox on but declares nothing to publish.
    /// Lists the context's event types so the fix is clear.
    /// </summary>
    internal static string NothingDeclaredMessage(string contextName, EventTypeCatalog? catalog)
    {
        var message =
            $"Bounded context '{contextName}' declares no published events. " +
            "Nothing leaves a context unless it is declared. Call PublishEvents(p => p.Publish<TEvent>() " +
            "or p.Translate<TInternal, TPublic>(...)) for the events other contexts may read.";

        if (catalog is null)
            return message;

        var families = catalog.EnumerateFamilies()
            .Select(f => $"{f.Token} ({f.Current.FullName})")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        if (families.Count == 0)
            return message;

        return message + " Event types in this context: " + string.Join(", ", families) + ".";
    }
}
