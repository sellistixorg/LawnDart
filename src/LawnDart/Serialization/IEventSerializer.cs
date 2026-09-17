namespace LawnDart.Serialization;

/// <summary>
/// Session payload codec. The durable log stores these bytes plus the
/// <c>EventCodec</c> id for <see cref="ContentType"/>; it does not know CLR
/// event types.
/// </summary>
/// <remarks>
/// One codec per session. The shipped default is UTF-8 JSON
/// (<c>application/json</c>). Binary codecs (protobuf, Avro, MemoryPack)
/// return raw payload bytes — not a Base64 string.
/// Metadata JSON is mapped by <c>EventSession</c>, not by this interface.
/// </remarks>
public interface IEventSerializer
{
    /// <summary>
    /// Plugin identity (e.g. <c>application/json</c>,
    /// <c>application/protobuf</c>, <c>application/avro</c>).
    /// Mapped to a frame <c>byte</c> via <c>EventCodec.IdFor</c>. Not stored
    /// as a MIME string on the log.
    /// </summary>
    string ContentType { get; }

    /// <summary>
    /// Serialize an object using its runtime type.
    /// </summary>
    /// <param name="obj">Object to serialize (typically <c>IEvent</c>).</param>
    /// <param name="type">Runtime type of the object.</param>
    /// <returns>Owned payload bytes. Callers may treat the memory as a snapshot.</returns>
    ReadOnlyMemory<byte> Serialize(object obj, Type type);

    /// <summary>
    /// Deserialize payload bytes to the specified type.
    /// </summary>
    /// <param name="data">Serialized payload bytes.</param>
    /// <param name="type">Target type for deserialization.</param>
    /// <returns>Deserialized object instance.</returns>
    object Deserialize(ReadOnlyMemory<byte> data, Type type);
}
