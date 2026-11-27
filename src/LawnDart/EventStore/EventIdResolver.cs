using System.Text.Json;

namespace LawnDart.EventStore;

/// <summary>
/// Reads <c>EventId</c> from append metadata. A missing or unreadable id is minted once
/// by the caller so a retried commit uses the same value. Minted ids are UUID version 7,
/// so they start with the time they were made and newer ids sort after older ones.
/// </summary>
internal static class EventIdResolver
{
    public static Guid Resolve(ReadOnlyMemory<byte> metadata, Func<Guid>? mint = null)
    {
        if (TryRead(metadata, out var id))
            return id;
        return mint is null ? Guid.CreateVersion7() : mint();
    }

    /// <summary>
    /// One id per envelope. A repeated metadata <c>EventId</c> in the same batch
    /// (one command metadata copied onto each event) is made distinct and stable
    /// for that batch: the first event keeps the id, and each repeat gets an id with the
    /// same version, variant and (for version 7) time, and different random bits.
    /// An id already stored is left unchanged so the caller can reject it.
    /// </summary>
    public static List<Guid> ResolveBatch(IReadOnlyList<ReadOnlyMemory<byte>> metadata)
    {
        var ids = new List<Guid>(metadata.Count);
        var seen = new HashSet<Guid>();
        for (var i = 0; i < metadata.Count; i++)
        {
            var id = Resolve(metadata[i]);
            if (!seen.Add(id))
            {
                var original = id;
                var salt = i + 1;
                do
                {
                    id = Derive(original, salt);
                    salt++;
                }
                while (!seen.Add(id));
            }

            ids.Add(id);
        }

        return ids;
    }

    /// <summary>
    /// Changes only the last four bytes, which are random bits in both version 4 and
    /// version 7 ids. The version, the variant and the version 7 time prefix stay the same,
    /// so a derived version 7 id still sorts by time. The same id and salt always give
    /// the same result, and different salts give different results.
    /// </summary>
    internal static Guid Derive(Guid id, int salt)
    {
        Span<byte> bytes = stackalloc byte[16];
        id.TryWriteBytes(bytes, bigEndian: true, out _);
        bytes[12] ^= (byte)(salt >> 24);
        bytes[13] ^= (byte)(salt >> 16);
        bytes[14] ^= (byte)(salt >> 8);
        bytes[15] ^= (byte)salt;
        return new Guid(bytes, bigEndian: true);
    }

    public static bool TryRead(ReadOnlyMemory<byte> metadata, out Guid eventId)
    {
        eventId = default;
        if (metadata.IsEmpty)
            return false;

        try
        {
            using var doc = JsonDocument.Parse(metadata);
            if (!doc.RootElement.TryGetProperty("EventId", out var property)
                || property.ValueKind != JsonValueKind.String)
                return false;

            return Guid.TryParse(property.GetString(), out eventId);
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
