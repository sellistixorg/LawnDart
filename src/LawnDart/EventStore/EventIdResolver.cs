using System.Text.Json;

namespace LawnDart.EventStore;

/// <summary>
/// Reads <c>EventId</c> from append metadata. A missing or unreadable id is minted once
/// by the caller so a retried commit uses the same value.
/// </summary>
internal static class EventIdResolver
{
    public static Guid Resolve(ReadOnlyMemory<byte> metadata, Func<Guid>? mint = null)
    {
        if (TryRead(metadata, out var id))
            return id;
        return (mint ?? Guid.NewGuid)();
    }

    /// <summary>
    /// One id per envelope. A repeated metadata <c>EventId</c> in the same batch
    /// (one command metadata copied onto each event) is made distinct and stable
    /// for that batch. An id already stored is left unchanged so the caller can reject it.
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
                var salt = i + 1;
                do
                {
                    id = Derive(id, salt);
                    salt++;
                }
                while (!seen.Add(id));
            }

            ids.Add(id);
        }

        return ids;
    }

    private static Guid Derive(Guid id, int salt)
    {
        var bytes = id.ToByteArray();
        bytes[0] = (byte)(bytes[0] + salt);
        bytes[1] ^= (byte)salt;
        return new Guid(bytes);
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
