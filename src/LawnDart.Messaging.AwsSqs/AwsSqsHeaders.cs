using System.Globalization;
using System.Text.Json;
using Amazon.SQS.Model;

namespace LawnDart.Messaging.AwsSqs;

/// <summary>
/// SQS and SNS message attributes for one LawnDart delivery.
/// SNS and SQS allow 10 message attributes. The envelope (user, tenant, custom headers,
/// and the other <see cref="MessageHeaders"/> fields) is one JSON attribute,
/// <see cref="Envelope"/>. The event type, attempt count, and deferral count are their own attributes.
/// When a message already has 10 attributes, the attempt and deferral counts are stored inside the envelope.
/// </summary>
public static class AwsSqsHeaders
{
    /// <summary>Declared event type. The value is <c>lawndart_type</c>.</summary>
    public const string EventType = AwsSqsNames.EventTypeAttribute;

    /// <summary>
    /// JSON object of envelope headers from <see cref="MessageEnvelopeHeaders"/>.
    /// The value is <c>lawndart-headers</c>.
    /// </summary>
    public const string Envelope = "lawndart-headers";

    /// <summary>Completed handler attempts. The value is <c>lawndart-attempt</c>.</summary>
    public const string Attempt = "lawndart-attempt";

    /// <summary>
    /// How many times a delivery has waited because its handler was not subscribed yet.
    /// The value is <c>lawndart-deferrals</c>.
    /// </summary>
    public const string Deferrals = "lawndart-deferrals";

    /// <summary>SNS and SQS allow this many message attributes.</summary>
    public const int MaxAttributes = 10;

    /// <summary>Attributes for a publish. The body stays the message JSON.</summary>
    /// <param name="context">Context to write.</param>
    /// <param name="declaredName">Declared message type name.</param>
    public static Dictionary<string, MessageAttributeValue> ForPublish(MessageContext context, string declaredName)
    {
        ArgumentNullException.ThrowIfNull(context);
        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        MessageEnvelopeHeaders.Write(context, declaredName, headers);
        return new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal)
        {
            [EventType] = Text(declaredName),
            [Envelope] = Text(JsonSerializer.Serialize(headers, MessagingJson.Options)),
        };
    }

    /// <summary>
    /// Attributes for a delayed resend to the same queue.
    /// Copies the attributes already on the message, then sets the attempt count.
    /// </summary>
    /// <param name="current">Attributes on the delivery. May be empty.</param>
    /// <param name="declaredName">Declared message type name.</param>
    /// <param name="completedAttempts">Handler attempts that have finished. A cancel passes the previous count.</param>
    /// <param name="deferrals">Deferral count to store, or <c>null</c> to leave the current value.</param>
    public static Dictionary<string, MessageAttributeValue> ForRetry(
        IReadOnlyDictionary<string, MessageAttributeValue> current,
        string declaredName,
        int completedAttempts,
        int? deferrals)
    {
        ArgumentNullException.ThrowIfNull(current);
        ArgumentException.ThrowIfNullOrWhiteSpace(declaredName);
        var copy = new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal);
        foreach (var pair in current)
        {
            if (pair.Value is null)
                continue;
            copy[pair.Key] = pair.Value;
        }

        var headers = ReadEnvelope(copy);
        var stashed = false;
        Remember(copy, headers, EventType, declaredName, ref stashed);
        Remember(copy, headers, Attempt, completedAttempts.ToString(CultureInfo.InvariantCulture), ref stashed);
        if (deferrals is int count)
            Remember(copy, headers, Deferrals, count.ToString(CultureInfo.InvariantCulture), ref stashed);

        if (stashed)
            WriteEnvelope(copy, headers);

        if (copy.Count > MaxAttributes)
        {
            throw new InvalidOperationException(
                $"The message has {copy.Count} attributes. SQS allows {MaxAttributes}.");
        }

        return copy;
    }

    private static void Remember(
        Dictionary<string, MessageAttributeValue> attributes,
        Dictionary<string, string> headers,
        string name,
        string value,
        ref bool stashed)
    {
        if (attributes.ContainsKey(name) || attributes.Count < MaxAttributes)
        {
            attributes[name] = Text(value);
            return;
        }

        headers[name] = value;
        stashed = true;
    }

    private static void WriteEnvelope(
        Dictionary<string, MessageAttributeValue> attributes,
        Dictionary<string, string> headers)
    {
        var json = JsonSerializer.Serialize(headers, MessagingJson.Options);
        if (attributes.ContainsKey(Envelope) || attributes.Count < MaxAttributes)
        {
            attributes[Envelope] = Text(json);
            return;
        }

        throw new InvalidOperationException(
            $"The message already has {MaxAttributes} attributes. SQS allows {MaxAttributes}. " +
            "LawnDart could not store the attempt count.");
    }

    /// <summary>How many handler attempts have already finished.</summary>
    /// <param name="attributes">Attributes on the delivery.</param>
    public static int CompletedAttempts(IReadOnlyDictionary<string, MessageAttributeValue> attributes)
        => ReadCount(attributes, Attempt);

    /// <summary>How many times this delivery has waited for a handler that was not subscribed yet.</summary>
    /// <param name="attributes">Attributes on the delivery.</param>
    public static int DeferralCount(IReadOnlyDictionary<string, MessageAttributeValue> attributes)
        => ReadCount(attributes, Deferrals);

    /// <summary>
    /// Envelope header map for <see cref="MessageEnvelopeHeaders.Read"/>.
    /// Attempt and deferral counts are left out.
    /// </summary>
    /// <param name="attributes">Attributes on the delivery.</param>
    public static Dictionary<string, string> ToEnvelope(IReadOnlyDictionary<string, MessageAttributeValue> attributes)
    {
        ArgumentNullException.ThrowIfNull(attributes);
        var result = ReadEnvelope(attributes);
        result.Remove(Attempt);
        result.Remove(Deferrals);
        result.Remove(EventType);
        return result;
    }

    /// <summary>Declared event type attribute, or <c>null</c> when it is absent.</summary>
    /// <param name="attributes">Attributes on the delivery.</param>
    public static string? EventTypeName(IReadOnlyDictionary<string, MessageAttributeValue> attributes)
    {
        ArgumentNullException.ThrowIfNull(attributes);
        return ReadText(attributes, EventType);
    }

    private static int ReadCount(IReadOnlyDictionary<string, MessageAttributeValue> attributes, string name)
    {
        ArgumentNullException.ThrowIfNull(attributes);
        if (attributes.TryGetValue(name, out var value) && value is not null)
        {
            var text = value.StringValue;
            if (string.IsNullOrWhiteSpace(text))
                return 0;
            if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) && parsed >= 0)
                return parsed;

            throw new InvalidOperationException(
                $"Message attribute '{name}' could not be read. The value was '{text}'.");
        }

        var headers = ReadEnvelope(attributes);
        if (!headers.TryGetValue(name, out var folded) || string.IsNullOrWhiteSpace(folded))
            return 0;
        if (int.TryParse(folded, NumberStyles.Integer, CultureInfo.InvariantCulture, out var fromEnvelope) && fromEnvelope >= 0)
            return fromEnvelope;

        throw new InvalidOperationException(
            $"Header '{name}' could not be read. The value was '{folded}'.");
    }

    private static Dictionary<string, string> ReadEnvelope(IReadOnlyDictionary<string, MessageAttributeValue> attributes)
    {
        if (!attributes.TryGetValue(Envelope, out var value) || string.IsNullOrWhiteSpace(value?.StringValue))
            return new Dictionary<string, string>(StringComparer.Ordinal);

        try
        {
            var headers = JsonSerializer.Deserialize<Dictionary<string, string>>(value.StringValue, MessagingJson.Options);
            return headers is null
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                : new Dictionary<string, string>(headers, StringComparer.Ordinal);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException(
                $"Message attribute '{Envelope}' must be a JSON object of strings.",
                ex);
        }
    }

    private static string? ReadText(IReadOnlyDictionary<string, MessageAttributeValue> attributes, string name)
    {
        var direct = ReadAttribute(attributes, name);
        if (direct is not null)
            return direct;

        var headers = ReadEnvelope(attributes);
        return headers.TryGetValue(name, out var folded) && !string.IsNullOrWhiteSpace(folded) ? folded : null;
    }

    private static string? ReadAttribute(IReadOnlyDictionary<string, MessageAttributeValue> attributes, string name)
    {
        if (!attributes.TryGetValue(name, out var value))
            return null;
        var text = value?.StringValue;
        return string.IsNullOrWhiteSpace(text) ? null : text;
    }

    private static MessageAttributeValue Text(string value) => new()
    {
        DataType = "String",
        StringValue = value,
    };
}
