using System.Globalization;

namespace LawnDart.Messaging;

/// <summary>
/// A declared message type name plus the <see cref="MessageContext"/> restored from headers.
/// </summary>
public sealed class MessageEnvelope
{
    /// <summary>
    /// Creates an envelope.
    /// </summary>
    /// <param name="messageType">Declared type name, or <c>null</c> when the header is absent.</param>
    /// <param name="context">Context restored from the headers.</param>
    public MessageEnvelope(string? messageType, MessageContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        MessageType = messageType;
        Context = context;
    }

    /// <summary>Declared message type name from <see cref="MessageHeaders.MessageType"/>.</summary>
    public string? MessageType { get; }

    /// <summary>Context restored from the envelope headers.</summary>
    public MessageContext Context { get; }
}

/// <summary>
/// Copies <see cref="MessageContext"/> to and from transport headers.
/// </summary>
public static class MessageEnvelopeHeaders
{
    /// <summary>
    /// Writes <paramref name="context"/> and <paramref name="messageType"/> into <paramref name="destination"/>.
    /// Typed ids overwrite a header of the same name. Other headers are stored under
    /// <see cref="MessageHeaders.CustomPrefix"/> unless they are trace or outbox headers.
    /// </summary>
    /// <param name="context">Context to write.</param>
    /// <param name="messageType">Declared message type name.</param>
    /// <param name="destination">Header map. Existing keys of the same name are replaced.</param>
    public static void Write(
        MessageContext context,
        string messageType,
        IDictionary<string, string> destination)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);
        ArgumentNullException.ThrowIfNull(destination);

        destination[MessageHeaders.MessageType] = messageType;
        Set(destination, MessageHeaders.MessageId, context.MessageId);
        Set(destination, MessageHeaders.CorrelationId, context.CorrelationId);
        Set(destination, MessageHeaders.CausationId, context.CausationId);
        Set(destination, MessageHeaders.UserId, context.UserId);
        Set(destination, MessageHeaders.TenantId, context.TenantId);
        destination[MessageHeaders.EnqueuedAt] = context.EnqueuedAt.ToString("o", CultureInfo.InvariantCulture);

        foreach (var pair in context.Headers)
        {
            if (IsReserved(pair.Key))
            {
                if (!destination.ContainsKey(pair.Key))
                    destination[pair.Key] = pair.Value;
                continue;
            }

            destination[MessageHeaders.CustomPrefix + pair.Key] = pair.Value;
        }
    }

    /// <summary>
    /// Restores a <see cref="MessageEnvelope"/> from <paramref name="headers"/>.
    /// Application headers lose <see cref="MessageHeaders.CustomPrefix"/>.
    /// Trace and outbox headers stay under their original names.
    /// </summary>
    /// <param name="headers">Headers read from the transport.</param>
    /// <param name="transportType">Value for <see cref="MessageContext.TransportType"/>.</param>
    public static MessageEnvelope Read(
        IReadOnlyDictionary<string, string> headers,
        string? transportType = null)
    {
        ArgumentNullException.ThrowIfNull(headers);

        var bag = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in headers)
        {
            if (IsPropertyHeader(pair.Key))
                continue;

            if (pair.Key.StartsWith(MessageHeaders.CustomPrefix, StringComparison.Ordinal))
            {
                var key = pair.Key[MessageHeaders.CustomPrefix.Length..];
                if (key.Length > 0)
                    bag[key] = pair.Value;
                continue;
            }

            bag[pair.Key] = pair.Value;
        }

        var context = new MessageContext
        {
            MessageId = Get(headers, MessageHeaders.MessageId),
            CorrelationId = Get(headers, MessageHeaders.CorrelationId),
            CausationId = Get(headers, MessageHeaders.CausationId),
            UserId = Get(headers, MessageHeaders.UserId),
            TenantId = Get(headers, MessageHeaders.TenantId),
            EnqueuedAt = TryGetEnqueuedAt(headers) ?? DateTimeOffset.UtcNow,
            TransportType = transportType,
            Headers = bag,
        };

        return new MessageEnvelope(Get(headers, MessageHeaders.MessageType), context);
    }

    private static void Set(IDictionary<string, string> destination, string name, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value))
            destination[name] = value;
    }

    private static string? Get(IReadOnlyDictionary<string, string> headers, string name)
    {
        if (!headers.TryGetValue(name, out var value) || string.IsNullOrWhiteSpace(value))
            return null;
        return value;
    }

    private static DateTimeOffset? TryGetEnqueuedAt(IReadOnlyDictionary<string, string> headers)
    {
        if (!headers.TryGetValue(MessageHeaders.EnqueuedAt, out var value) || string.IsNullOrWhiteSpace(value))
            return null;

        if (!DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed))
        {
            throw new InvalidOperationException(
                $"Header '{MessageHeaders.EnqueuedAt}' must be a round-trip date and time. The value was '{value}'.");
        }

        return parsed;
    }

    private static bool IsPropertyHeader(string key) =>
        key is MessageHeaders.MessageId
            or MessageHeaders.MessageType
            or MessageHeaders.CorrelationId
            or MessageHeaders.CausationId
            or MessageHeaders.UserId
            or MessageHeaders.TenantId
            or MessageHeaders.EnqueuedAt;

    private static bool IsReserved(string key) =>
        IsPropertyHeader(key)
        || key is MessageHeaders.TraceParent
            or MessageHeaders.TraceState
            or MessageHeaders.OutboxStreamId
            or MessageHeaders.OutboxSequencePosition
            or MessageHeaders.OutboxEventType;
}
