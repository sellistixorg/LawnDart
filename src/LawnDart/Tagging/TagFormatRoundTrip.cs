namespace LawnDart.Tagging;

/// <summary>
/// Checks whether a tag is a string <see cref="ITagFormat"/> would have produced.
/// </summary>
public static class TagFormatRoundTrip
{
    /// <summary>
    /// Returns true when <paramref name="tag"/> equals the string
    /// <paramref name="format"/> builds for its type and id.
    /// </summary>
    /// <param name="format">The context format.</param>
    /// <param name="tag">The tag to check.</param>
    /// <returns>
    /// True when the tag round-trips. False when it has no colon, the id is empty,
    /// or the formatted string differs.
    /// </returns>
    /// <remarks>
    /// The type is the text before the first colon. The id is the rest.
    /// An id that parses as a <see cref="Guid"/> uses <see cref="ITagFormat.Tag(string, Guid)"/>.
    /// Any other id uses <see cref="ITagFormat.Tag(string, string)"/>.
    /// A 32-hex-digit id is treated as a <see cref="Guid"/>.
    /// </remarks>
    public static bool Matches(ITagFormat format, string tag)
    {
        ArgumentNullException.ThrowIfNull(format);
        if (string.IsNullOrEmpty(tag))
            return false;

        var colon = tag.IndexOf(':');
        if (colon <= 0 || colon == tag.Length - 1)
            return false;

        var type = tag[..colon];
        var id = tag[(colon + 1)..];
        if (Guid.TryParse(id, out var guid))
            return string.Equals(tag, format.Tag(type, guid), StringComparison.Ordinal);

        return string.Equals(tag, format.Tag(type, id), StringComparison.Ordinal);
    }
}
