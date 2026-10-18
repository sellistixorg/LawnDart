using Microsoft.Extensions.Logging;

namespace LawnDart.Tagging;

/// <summary>
/// One context's record of tag types that did not round-trip.
/// </summary>
/// <remarks>
/// Registered once per bounded context. The key is the tag type (the text
/// before the first colon), so each new id of a known bad type does not
/// grow the record. After <see cref="DefaultMaxTypes"/> types, further
/// unknown types are not logged.
/// </remarks>
internal sealed class UnknownTagWarningLog
{
    internal const int DefaultMaxTypes = 128;

    private readonly int _maxTypes;
    private readonly object _gate = new();
    private readonly HashSet<string> _types = new(StringComparer.Ordinal);
    private bool _suppressed;

    internal UnknownTagWarningLog(int maxTypes = DefaultMaxTypes)
    {
        if (maxTypes < 1)
            throw new ArgumentOutOfRangeException(nameof(maxTypes), "The tag-type cap must be at least 1.");
        _maxTypes = maxTypes;
    }

    /// <summary>
    /// Logs one warning per tag type that does not round-trip.
    /// Does not throw and does not change <paramref name="tags"/>.
    /// </summary>
    internal void Warn(ILogger logger, ITagFormat format, IEnumerable<string>? tags)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(format);
        if (tags is null)
            return;

        foreach (var tag in tags)
        {
            if (string.IsNullOrEmpty(tag))
                continue;

            bool matches;
            try
            {
                matches = TagFormatRoundTrip.Matches(format, tag);
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "Tag format check failed for {Tag}.", tag);
                matches = false;
            }

            if (matches)
                continue;

            switch (Decide(TypeKey(tag)))
            {
                case WarnDecision.Log:
                    logger.LogWarning(
                        "Tag {Tag} does not round-trip through the context tag format. Loads, fences, and later search compare the whole string.",
                        tag);
                    break;
                case WarnDecision.Suppress:
                    logger.LogWarning(
                        "Further unknown tag warnings are suppressed. This context already logged {MaxTypes} tag types.",
                        _maxTypes);
                    break;
            }
        }
    }

    internal static string TypeKey(string tag)
    {
        var colon = tag.IndexOf(':');
        return colon > 0 ? tag[..colon] : tag;
    }

    private WarnDecision Decide(string type)
    {
        lock (_gate)
        {
            if (!_types.Add(type))
                return WarnDecision.Skip;

            if (_types.Count <= _maxTypes)
                return WarnDecision.Log;

            _types.Remove(type);
            if (_suppressed)
                return WarnDecision.Skip;

            _suppressed = true;
            return WarnDecision.Suppress;
        }
    }

    private enum WarnDecision
    {
        Skip,
        Log,
        Suppress
    }
}
