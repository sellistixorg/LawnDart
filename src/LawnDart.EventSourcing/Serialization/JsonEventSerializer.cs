using System.Text.Json;
using System.Text.Json.Serialization;
using LawnDart.Serialization;

namespace LawnDart.EventSourcing.Serialization;

/// <summary>
/// Default session payload codec: System.Text.Json as UTF-8 bytes.
/// Does not require <c>PropertyOrderAttribute</c>.
/// </summary>
public class JsonEventSerializer : IEventSerializer
{
    private readonly JsonSerializerOptions _options;

    public string ContentType => "application/json";

    public JsonEventSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions
        {
            // Using PascalCase (not camelCase) for positional record compatibility
            PropertyNameCaseInsensitive = true,
            WriteIndented = false,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };
    }

    public ReadOnlyMemory<byte> Serialize(object obj, Type type)
    {
        return JsonSerializer.SerializeToUtf8Bytes(obj, type, _options);
    }

    public object Deserialize(ReadOnlyMemory<byte> data, Type type)
    {
        return JsonSerializer.Deserialize(data.Span, type, _options)
            ?? throw new InvalidOperationException($"Failed to deserialize {type.Name}");
    }
}
