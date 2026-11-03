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
    internal static IReadOnlyList<string> SnapshotTags(IEnumerable<string>? tags)
    {
        if (tags is null)
            return [];

        var copy = new List<string>(tags is ICollection<string> collection ? collection.Count : 4);
        HashSet<string>? seen = null;
        foreach (var tag in tags)
        {
            if (copy.Count == 0)
            {
                copy.Add(tag);
                continue;
            }

            seen ??= new HashSet<string>(copy, StringComparer.Ordinal);
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

    internal static string RequireStreamId(string streamId)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream id is required.", nameof(streamId));

        return streamId;
    }
}
