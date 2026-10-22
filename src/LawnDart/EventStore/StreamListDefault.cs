namespace LawnDart.EventStore;

/// <summary>
/// Default page for a registry that does not implement <see cref="IStreamRegistry.ListStreamsAsync"/>.
/// Loads the matching streams, sorts by stream id, and returns one page.
/// </summary>
internal static class StreamListDefault
{
    internal static async Task<StreamPage> ListAsync(
        IStreamRegistry registry,
        StreamListFilter? filter,
        string? pageToken,
        int pageSize,
        CancellationToken cancellationToken)
    {
        StreamPageToken.CheckPageSize(pageSize);
        var after = StreamPageToken.ReadAfter(pageToken, filter);
        var prefix = StreamPageToken.Normalize(filter?.Prefix);
        var tag = StreamPageToken.Normalize(filter?.Tag);
        var aggregateType = StreamPageToken.Normalize(filter?.AggregateType);

        var ids = await registry.EnumerateStreamIdsAsync(prefix, cancellationToken).ConfigureAwait(false);
        var matches = new List<StreamMetadata>();
        foreach (var id in ids)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var metadata = await registry.GetStreamAsync(id, cancellationToken).ConfigureAwait(false);
            if (metadata is null || metadata.Status != StreamStatus.Active)
                continue;
            if (aggregateType is not null && !string.Equals(metadata.AggregateType, aggregateType, StringComparison.Ordinal))
                continue;
            if (tag is not null && !HasTag(metadata, tag))
                continue;
            if (after is not null && string.CompareOrdinal(metadata.StreamId, after) <= 0)
                continue;
            matches.Add(metadata);
        }

        matches.Sort(static (left, right) => string.CompareOrdinal(left.StreamId, right.StreamId));
        if (matches.Count > pageSize + 1)
            matches.RemoveRange(pageSize + 1, matches.Count - (pageSize + 1));

        return StreamPageToken.ToPage(matches, pageSize, filter);
    }

    private static bool HasTag(StreamMetadata metadata, string tag)
    {
        foreach (var candidate in metadata.Tags)
        {
            if (string.Equals(candidate, tag, StringComparison.Ordinal))
                return true;
        }

        return false;
    }
}
