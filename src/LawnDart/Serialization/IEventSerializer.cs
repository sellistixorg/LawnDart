namespace LawnDart.Serialization;

/// <summary>
/// Abstraction for event serialization/deserialization.
/// Allows plugging different serializers (JSON, Protobuf, Avro, etc.)
/// </summary>
public interface IEventSerializer
{
    /// <summary>
    /// Content type identifier (e.g., "application/json", "application/protobuf", "application/avro").
    /// Stored in database to support format detection during deserialization.
    /// </summary>
    string ContentType { get; }

    /// <summary>
    /// Serialize an object using its runtime type.
    /// For binary formats, returns Base64-encoded string for SQL Server storage.
    /// </summary>
    /// <param name="obj">Object to serialize (typically IEvent or ICommand)</param>
    /// <param name="type">Runtime type of the object</param>
    /// <returns>Serialized representation (JSON string or Base64-encoded binary)</returns>
    string Serialize(object obj, Type type);

    /// <summary>
    /// Deserialize an object to the specified type.
    /// Handles Base64 decoding for binary formats automatically.
    /// </summary>
    /// <param name="data">Serialized data (JSON string or Base64-encoded binary)</param>
    /// <param name="type">Target type for deserialization</param>
    /// <returns>Deserialized object instance</returns>
    object Deserialize(string data, Type type);
}
