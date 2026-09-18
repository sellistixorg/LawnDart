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

    internal static IReadOnlyList<string> SnapshotTags(IEnumerable<string>? tags)
    {
        if (tags is null)
            return [];

        return tags as string[] is { } array
            ? [.. array]
            : [.. tags];
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
