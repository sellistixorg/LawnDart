namespace LawnDart.Tagging;

/// <summary>
/// Builds one tag string from a type and an id.
/// </summary>
/// <remarks>
/// Append, load, <c>Emit</c>, and the append fence call the same format.
/// A different spelling is a different tag.
/// </remarks>
public interface ITagFormat
{
    /// <summary>
    /// Builds a tag for <paramref name="type"/> and <paramref name="id"/>.
    /// </summary>
    /// <param name="type">Tag type, such as <c>student</c>.</param>
    /// <param name="id">Identity.</param>
    /// <returns>The tag string.</returns>
    string Tag(string type, Guid id);

    /// <summary>
    /// Builds a tag for <paramref name="type"/> and <paramref name="id"/>.
    /// </summary>
    /// <param name="type">Tag type, such as <c>tenant</c>.</param>
    /// <param name="id">Identity text. The format decides whether to rewrite it.</param>
    /// <returns>The tag string.</returns>
    string Tag(string type, string id);
}
