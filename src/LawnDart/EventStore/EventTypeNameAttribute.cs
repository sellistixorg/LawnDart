namespace LawnDart.EventStore;

/// <summary>
/// Declares the stable catalog token stored in the event log for this type.
/// Required on every concrete <see cref="IEvent"/> that is written.
/// Prefer a kebab-case token (e.g. <c>"order-placed"</c>).
/// </summary>
/// <remarks>
/// <para>
/// The token is a <strong>family</strong> name. It does not change when the
/// payload shape versions. <see cref="EventTypeCatalog.GetName"/> returns that
/// token, not <c>token.v2</c>.
/// </para>
/// <para>
/// One-arg <c>[EventTypeName("token")]</c> is schema version 1 and is
/// implicitly current when it is the only type for that family. A family with
/// two or more types needs exactly one <c>current: true</c>. Do not infer
/// current from the highest version. The current CLR type keeps the domain
/// name (<c>AuthorRegistered</c>); historical types are
/// <c>AuthorRegisteredV1</c>.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// // Single type: version 1, implicitly current.
/// [EventTypeName("author-registered")]
/// public record AuthorRegistered(...) : IEvent { ... }
///
/// // After a breaking shape change: current keeps the domain name.
/// [EventTypeName("author-registered", version: 1)]
/// public record AuthorRegisteredV1(...) : IEvent { ... }
///
/// [EventTypeName("author-registered", version: 2, current: true)]
/// public record AuthorRegistered(...) : IEvent { ... }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class EventTypeNameAttribute : Attribute
{
    /// <summary>
    /// The stable family token stored in the event log for this event type.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Schema version for this CLR type. One-arg construction is version 1.
    /// </summary>
    public int Version { get; }

    /// <summary>
    /// Marks this type as the current CLR type for the family. Required once
    /// a token has two or more types. Ignored when the family has one type
    /// (that type is implicitly current). Defaults to <c>false</c> on the
    /// versioned constructor.
    /// </summary>
    public bool Current { get; }

    /// <summary>
    /// Initializes the attribute with a family token. Schema version is 1.
    /// Implicitly current when this is the only type for the token.
    /// </summary>
    /// <param name="name">
    /// The stable family token to store (e.g. <c>"order-placed"</c> or
    /// <c>"author-registered"</c>).
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="name"/> is <c>null</c>.</exception>
    public EventTypeNameAttribute(string name)
        : this(name, version: 1, current: false)
    {
    }

    /// <summary>
    /// Initializes the attribute with a family token and schema version.
    /// </summary>
    /// <param name="name">The stable family token to store.</param>
    /// <param name="version">Schema version for this CLR type. Must be at least 1.</param>
    /// <param name="current">
    /// <c>true</c> when this is the current type for the family. Required
    /// once a token has two or more types. Defaults to <c>false</c>.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="name"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="version"/> is less than 1.</exception>
    public EventTypeNameAttribute(string name, int version, bool current = false)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        if (version < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(version),
                version,
                "Schema version must be at least 1.");
        }

        Version = version;
        Current = current;
    }
}
