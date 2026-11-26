namespace LawnDart.EventStore;

/// <summary>
/// Limits which streams a page includes. Each set property must match.
/// A null or empty string property is not applied; when <see cref="Tag"/> is set,
/// it must be built with <see cref="EventStore.Tag.Create"/> or <see cref="EventStore.Tag.Parse"/>.
/// </summary>
public sealed record StreamListFilter
{
    private readonly Tag? _tag;

    /// <summary>
    /// Stream id prefix. The tenant id stays in this prefix, as it does for
    /// <see cref="IStreamRegistry.EnumerateStreamIdsAsync"/>.
    /// <c>_</c>, <c>%</c>, and <c>[</c> are literal characters.
    /// </summary>
    public string? Prefix { get; init; }

    /// <summary>
    /// Stream tag. The match is the whole tag.
    /// </summary>
    public Tag? Tag
    {
        get => _tag;
        init => _tag = value is { } tag ? EventLogBuffers.RequireTag(tag) : null;
    }

    /// <summary>
    /// Aggregate type name stored on the stream.
    /// </summary>
    public string? AggregateType { get; init; }
}
