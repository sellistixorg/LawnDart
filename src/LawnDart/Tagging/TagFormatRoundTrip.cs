using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace LawnDart.Tagging;

/// <summary>
/// Checks whether a tag is a string <see cref="ITagFormat"/> would have produced.
/// </summary>
public static class TagFormatRoundTrip
{
    private static readonly ConcurrentDictionary<string, byte> Warned = new(StringComparer.Ordinal);

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

    /// <summary>
    /// Logs one warning per distinct tag that does not round-trip.
    /// </summary>
    /// <param name="logger">Warning destination. No log is written when this is null.</param>
    /// <param name="format">The context format. The check is skipped when this is null.</param>
    /// <param name="warn">When false, the check is skipped.</param>
    /// <param name="tags">Tags to check.</param>
    /// <remarks>
    /// The log is once per distinct string for the process. The method does not throw.
    /// A format that throws is treated as a mismatch and does not escape.
    /// </remarks>
    public static void WarnOnce(ILogger? logger, ITagFormat? format, bool warn, IEnumerable<string>? tags)
    {
        if (!warn || logger is null || format is null || tags is null)
            return;

        foreach (var tag in tags)
        {
            if (string.IsNullOrEmpty(tag))
                continue;

            bool matches;
            try
            {
                matches = Matches(format, tag);
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "Tag format check failed for {Tag}.", tag);
                matches = false;
            }

            if (matches)
                continue;

            if (!Warned.TryAdd(tag, 0))
                continue;

            logger.LogWarning(
                "Tag {Tag} does not round-trip through the context tag format. Loads, fences, and later search compare the whole string.",
                tag);
        }
    }
}
