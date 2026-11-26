namespace LawnDart.EventStore;

/// <summary>
/// Snapshots caller-owned buffers so a log envelope cannot alias a mutable array
/// the caller still holds (L18).
/// </summary>
internal static class EventLogBuffers
{
    internal static ReadOnlyMemory<byte> Snapshot(ReadOnlyMemory<byte> source)
    {
        if (source.IsEmpty)
            return ReadOnlyMemory<byte>.Empty;

        return source.ToArray();
    }

    /// <summary>
    /// Copies <paramref name="tags"/> and drops repeats. The first copy of each tag
    /// stays, in its original order. Tags compare exactly (ordinal): <c>A:1</c> and
    /// <c>a:1</c> are two tags, and no spaces are trimmed.
    /// </summary>
    internal static IReadOnlyList<Tag> SnapshotTags(IEnumerable<Tag>? tags)
    {
        if (tags is null)
            return [];

        var copy = new List<Tag>(tags is ICollection<Tag> collection ? collection.Count : 4);
        HashSet<Tag>? seen = null;
        foreach (var tag in tags)
        {
            RequireTag(tag);
            if (copy.Count == 0)
            {
                copy.Add(tag);
                continue;
            }

            seen ??= new HashSet<Tag>(copy);
            if (seen.Add(tag))
                copy.Add(tag);
        }

        return copy.ToArray();
    }

    internal static int NormalizeSchemaVersion(int schemaVersion)
    {
        if (schemaVersion < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(schemaVersion),
                schemaVersion,
                "SchemaVersion cannot be negative.");
        }

        return schemaVersion == 0 ? 1 : schemaVersion;
    }

    internal static string RequireEventType(string eventType)
    {
        if (string.IsNullOrWhiteSpace(eventType))
            throw new ArgumentException("Family token (event type) is required.", nameof(eventType));

        return eventType;
    }

    internal static byte RequireCodecId(byte codecId)
    {
        if (codecId == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(codecId),
                codecId,
                "Codec id 0 is never valid on a durable frame.");
        }

        return codecId;
    }

    internal static StreamId RequireStreamId(StreamId streamId)
    {
        if (streamId.IsDefault)
            throw new ArgumentException("Stream id is required. Build one with StreamId.For, StreamId.Dcb or StreamId.Parse.", nameof(streamId));

        return streamId;
    }

    internal static Tag RequireTag(Tag tag)
    {
        if (tag.IsDefault)
            throw new ArgumentException("Tag is required. Build one with Tag.Create or Tag.Parse.", nameof(tag));

        return tag;
    }

    internal static IReadOnlyList<Tag> RequireTags(IReadOnlyList<Tag> tags)
    {
        for (var i = 0; i < tags.Count; i++)
            RequireTag(tags[i]);

        return tags;
    }
}
