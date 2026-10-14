using System.Globalization;

namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// Reads RabbitMQ header tables into envelope strings.
/// Broker headers that start with <c>x-</c> stay off <see cref="MessageContext.Headers"/>.
/// </summary>
internal static class RabbitMqHeaders
{
    public const string DeliveryCount = "x-delivery-count";

    /// <summary>Completed handler attempts carried across a retry delay. The value is <c>x-lawndart-attempt</c>.</summary>
    public const string Attempt = "x-lawndart-attempt";

    public static Dictionary<string, string> ToEnvelope(IReadOnlyDictionary<string, object?>? headers)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (headers is null)
            return result;

        foreach (var pair in headers)
        {
            if (pair.Key.StartsWith("x-", StringComparison.Ordinal))
                continue;
            if (pair.Value is null)
                continue;

            var text = ToText(pair.Value);
            if (text is not null)
                result[pair.Key] = text;
        }

        return result;
    }

    public static int ReadDeliveryCount(IReadOnlyDictionary<string, object?>? headers)
    {
        if (headers is null || !headers.TryGetValue(DeliveryCount, out var value) || value is null)
            return 0;

        return ToInt(DeliveryCount, value);
    }

    /// <summary>
    /// How many handler attempts have already finished.
    /// Prefers <see cref="Attempt"/> when this transport wrote it. Otherwise uses <see cref="DeliveryCount"/>.
    /// </summary>
    public static int CompletedAttempts(IReadOnlyDictionary<string, object?>? headers)
    {
        if (headers is not null && headers.TryGetValue(Attempt, out var value) && value is not null)
            return ToInt(Attempt, value);

        return ReadDeliveryCount(headers);
    }

    private static int ToInt(string name, object value)
    {
        if (value is byte[] bytes &&
            int.TryParse(System.Text.Encoding.UTF8.GetString(bytes), NumberStyles.Integer, CultureInfo.InvariantCulture, out var fromBytes))
        {
            return fromBytes;
        }

        try
        {
            return Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
        {
            throw new InvalidOperationException(
                $"Header '{name}' could not be read. The runtime type was '{value.GetType().FullName}'.",
                ex);
        }
    }

    public static Dictionary<string, object?>? Copy(object? headers)
    {
        if (headers is not System.Collections.IEnumerable rows)
            return null;

        var copy = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (headers is IEnumerable<KeyValuePair<string, object?>> typed)
        {
            foreach (var pair in typed)
                copy[pair.Key] = pair.Value;
            return copy.Count == 0 ? null : copy;
        }

        foreach (var row in rows)
        {
            if (row is System.Collections.DictionaryEntry entry && entry.Key is string key)
                copy[key] = entry.Value;
        }

        return copy.Count == 0 ? null : copy;
    }

    private static string? ToText(object value) => value switch
    {
        string text => text,
        byte[] bytes => System.Text.Encoding.UTF8.GetString(bytes),
        ReadOnlyMemory<byte> memory => System.Text.Encoding.UTF8.GetString(memory.Span),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture),
    };
}
