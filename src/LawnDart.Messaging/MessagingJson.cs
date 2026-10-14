using System.Text.Json;
using System.Text.Json.Serialization;

namespace LawnDart.Messaging;

/// <summary>
/// The JSON settings for message bodies and outbox hydration.
/// </summary>
/// <remarks>
/// Property names are PascalCase, the same shape the event store writes.
/// Matching ignores case. Null properties are omitted.
/// Every transport and the outbox publisher use <see cref="Options"/>.
/// The instance is read-only.
/// </remarks>
public static class MessagingJson
{
    /// <summary>
    /// Shared serializer settings. Do not build a second <see cref="JsonSerializerOptions"/> for messages.
    /// </summary>
    public static JsonSerializerOptions Options { get; } = CreateOptions();

    private static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            WriteIndented = false,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
        options.MakeReadOnly(populateMissingResolver: true);
        return options;
    }
}
