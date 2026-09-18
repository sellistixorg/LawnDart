using System.Diagnostics.CodeAnalysis;

namespace LawnDart.EventStore;

/// <summary>
/// Process-wide intern table from payload MIME (plugin identity on
/// <c>IEventSerializer.ContentType</c>) to the <c>byte</c> stored on a log frame.
/// </summary>
/// <remarks>
/// Known ids are never reused: <see cref="Json"/>, <see cref="MemoryPack"/>,
/// <see cref="Avro"/>, <see cref="Protobuf"/>. <c>1</c>–<c>63</c> are
/// LawnDart-reserved. <c>64</c>–<c>254</c> are user-registered.
/// <see cref="Opaque"/> (<c>255</c>) means unspecified bytes and never hydrates
/// on the typed path. <c>0</c> is never valid on a durable frame.
/// <para>
/// This table holds no domain data and no per-context state. An unregistered
/// MIME fails closed — it is never mapped to <see cref="Opaque"/> or to a
/// reserved id. Call <see cref="Register"/> to add a user codec.
/// </para>
/// </remarks>
public static class EventCodec
{
    /// <summary>UTF-8 JSON (STJ). MIME <see cref="JsonMime"/>.</summary>
    public const byte Json = 1;

    /// <summary>MemoryPack. MIME <see cref="MemoryPackMime"/>.</summary>
    public const byte MemoryPack = 2;

    /// <summary>Avro. MIME <see cref="AvroMime"/>.</summary>
    public const byte Avro = 3;

    /// <summary>Protobuf. MIME <see cref="ProtobufMime"/>.</summary>
    public const byte Protobuf = 4;

    /// <summary>Opaque / unspecified bytes. Never valid as a typed hydrate codec.</summary>
    public const byte Opaque = 255;

    /// <summary>Inclusive start of the user-registered id range.</summary>
    public const byte UserRangeStart = 64;

    /// <summary>Inclusive end of the user-registered id range.</summary>
    public const byte UserRangeEnd = 254;

    /// <summary>Plugin MIME for <see cref="Json"/>.</summary>
    public const string JsonMime = "application/json";

    /// <summary>Plugin MIME for <see cref="MemoryPack"/>.</summary>
    public const string MemoryPackMime = "application/vnd.lawndart.memorypack";

    /// <summary>Plugin MIME for <see cref="Avro"/>.</summary>
    public const string AvroMime = "application/avro";

    /// <summary>Plugin MIME for <see cref="Protobuf"/>.</summary>
    public const string ProtobufMime = "application/protobuf";

    private static readonly object Gate = new();
    private static readonly Dictionary<byte, string> IdToMime = new();
    private static readonly Dictionary<string, byte> MimeToId = new(StringComparer.OrdinalIgnoreCase);

    static EventCodec()
    {
        Seed(Json, JsonMime);
        Seed(MemoryPack, MemoryPackMime);
        Seed(Avro, AvroMime);
        Seed(Protobuf, ProtobufMime);
    }

    /// <summary>
    /// Returns the interned id for <paramref name="mime"/>.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="mime"/> is null or whitespace.</exception>
    /// <exception cref="UnregisteredEventCodecException">
    /// <paramref name="mime"/> is not built-in and was not passed to <see cref="Register"/>.
    /// </exception>
    public static byte IdFor(string mime)
    {
        if (string.IsNullOrWhiteSpace(mime))
            throw new ArgumentException("MIME is required.", nameof(mime));

        lock (Gate)
        {
            if (MimeToId.TryGetValue(mime, out var id))
                return id;
        }

        throw new UnregisteredEventCodecException(mime);
    }

    /// <summary>
    /// Looks up the plugin MIME for <paramref name="id"/>.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when <paramref name="id"/> is a built-in or
    /// user-registered codec. <see cref="Opaque"/> and <c>0</c> return
    /// <see langword="false"/>.
    /// </returns>
    public static bool TryGetMime(byte id, [NotNullWhen(true)] out string? mime)
    {
        lock (Gate)
        {
            return IdToMime.TryGetValue(id, out mime);
        }
    }

    /// <summary>
    /// Registers a user codec. Accepts ids <see cref="UserRangeStart"/>–
    /// <see cref="UserRangeEnd"/> only. Re-registering the same pair is
    /// idempotent; a conflicting pair throws.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="id"/> is outside the user range (reserved, <c>0</c>, or <see cref="Opaque"/>).
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="mime"/> is null or whitespace.</exception>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="id"/> or <paramref name="mime"/> is already bound to a different pair.
    /// </exception>
    public static void Register(byte id, string mime)
    {
        if (id < UserRangeStart || id > UserRangeEnd)
        {
            throw new ArgumentOutOfRangeException(
                nameof(id),
                id,
                $"Register accepts ids {UserRangeStart}–{UserRangeEnd}. " +
                "Ids 1–63 are LawnDart-reserved. 0 is never valid. 255 is Opaque.");
        }

        if (string.IsNullOrWhiteSpace(mime))
            throw new ArgumentException("MIME is required.", nameof(mime));

        lock (Gate)
        {
            if (IdToMime.TryGetValue(id, out var existingMime))
            {
                if (string.Equals(existingMime, mime, StringComparison.OrdinalIgnoreCase))
                    return;

                throw new InvalidOperationException(
                    $"Codec id {id} is already registered as '{existingMime}'.");
            }

            if (MimeToId.TryGetValue(mime, out var existingId))
            {
                throw new InvalidOperationException(
                    $"MIME '{mime}' is already registered as codec id {existingId}.");
            }

            IdToMime[id] = mime;
            MimeToId[mime] = id;
        }
    }

    /// <summary>Formats <paramref name="id"/> for exception text through the intern table.</summary>
    internal static string Describe(byte id)
    {
        if (id == Opaque)
            return "opaque / unspecified bytes";
        if (TryGetMime(id, out var mime))
            return mime;
        return $"unregistered:{id}";
    }

    private static void Seed(byte id, string mime)
    {
        IdToMime[id] = mime;
        MimeToId[mime] = id;
    }
}
