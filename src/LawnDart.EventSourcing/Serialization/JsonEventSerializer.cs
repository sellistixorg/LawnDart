using System.Text.Json;
using System.Text.Json.Serialization;
using LawnDart.Serialization;

namespace LawnDart.EventSourcing.Serialization;

/// <summary>
/// JSON-based event serializer.
/// Human-readable, debugging-friendly, and the default serializer.
/// Does not require PropertyOrderAttribute.
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

    public string Serialize(object obj, Type type)
    {
        return JsonSerializer.Serialize(obj, type, _options);
    }

    public object Deserialize(string data, Type type)
    {
        return JsonSerializer.Deserialize(data, type, _options)
            ?? throw new InvalidOperationException($"Failed to deserialize {type.Name}");
    }
}
