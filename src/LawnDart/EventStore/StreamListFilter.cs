namespace LawnDart.EventStore;

/// <summary>
/// Limits which streams a page includes. Each set property must match.
/// A null or empty property is not applied.
/// </summary>
public sealed record StreamListFilter
{
    /// <summary>
    /// Stream id prefix. The tenant id stays in this prefix, as it does for
    /// <see cref="IStreamRegistry.EnumerateStreamIdsAsync"/>.
    /// <c>_</c>, <c>%</c>, and <c>[</c> are literal characters.
    /// </summary>
    public string? Prefix { get; init; }

    /// <summary>
    /// Stream tag. The match is the whole tag.
    /// </summary>
    public string? Tag { get; init; }

    /// <summary>
    /// Aggregate type name stored on the stream.
    /// </summary>
    public string? AggregateType { get; init; }
}
