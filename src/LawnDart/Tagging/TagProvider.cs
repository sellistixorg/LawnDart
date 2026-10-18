namespace LawnDart.Tagging;

/// <summary>
/// Base class for a tag provider that builds every tag through one <see cref="ITagFormat"/>.
/// </summary>
/// <remarks>
/// Domain helpers, such as a method that returns the tags for one enrollment,
/// belong on the concrete provider. This type only forwards to the format.
/// </remarks>
public abstract class TagProvider : ITagProvider
{
    /// <summary>
    /// Creates a provider that builds tags with <paramref name="format"/>.
    /// </summary>
    /// <param name="format">The context format.</param>
    protected TagProvider(ITagFormat format)
    {
        ArgumentNullException.ThrowIfNull(format);
        Format = format;
    }

    /// <summary>
    /// The format this provider writes.
    /// </summary>
    protected ITagFormat Format { get; }

    /// <summary>
    /// Builds a tag for <paramref name="type"/> and <paramref name="id"/>.
    /// </summary>
    /// <param name="type">Tag type.</param>
    /// <param name="id">Identity.</param>
    /// <returns>The tag string from <see cref="Format"/>.</returns>
    public string Tag(string type, Guid id) => Format.Tag(type, id);

    /// <summary>
    /// Builds a tag for <paramref name="type"/> and <paramref name="id"/>.
    /// </summary>
    /// <param name="type">Tag type.</param>
    /// <param name="id">Identity text.</param>
    /// <returns>The tag string from <see cref="Format"/>.</returns>
    public string Tag(string type, string id) => Format.Tag(type, id);

    /// <inheritdoc />
    public abstract IEnumerable<string> GetTags(IEvent @event, object? context = null);
}
