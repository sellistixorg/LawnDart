using System.Text.Json;
using System.Text.Json.Serialization;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Outbox;

namespace LawnDart.Messaging.Outbox;

/// <summary>
/// Sample <see cref="IOutboxPublisher"/> that deserializes <see cref="OutboxMessage"/> payloads
/// and publishes them through <see cref="IMessageTransport"/> (InMemory or Azure Service Bus).
/// </summary>
/// <remarks>
/// <para>
/// Delivery is <b>at-least-once</b>. This adapter does not claim exactly-once semantics —
/// register an <see cref="IInboxStore"/> (or equivalent) on consumers and use a stable
/// <see cref="MessageContext.MessageId"/> (this type uses the outbox row id).
/// </para>
/// <para>
/// Event type resolution uses the catalog token from
/// <see cref="EventTypeNameResolver.GetName"/>. FullName and simple name remain
/// read aliases for older outbox rows.
/// </para>
/// </remarks>
public sealed class MessageTransportOutboxPublisher : IOutboxPublisher
{
    private static readonly JsonSerializerOptions DefaultJsonOptions = new()
    {
        // Match SqlServerEventStore outbox serialization (PascalCase / case-insensitive).
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly IMessageTransport _transport;
    private readonly IReadOnlyDictionary<string, Type> _eventTypes;
    private readonly JsonSerializerOptions _jsonOptions;

    /// <summary>
    /// Creates a publisher that resolves event CLR types from <paramref name="eventTypes"/>.
    /// </summary>
    /// <param name="transport">Transport used to publish (e.g. Service Bus or InMemory).</param>
    /// <param name="eventTypes">Known event types that may appear in outbox <c>EventType</c> rows.</param>
    /// <param name="jsonOptions">
    /// Optional JSON options. Defaults match SQL Server outbox payload serialization
    /// (<c>PropertyNameCaseInsensitive</c>, no camelCase rename).
    /// </param>
    public MessageTransportOutboxPublisher(
        IMessageTransport transport,
        IEnumerable<Type> eventTypes,
        JsonSerializerOptions? jsonOptions = null)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(eventTypes);

        _transport = transport;
        _jsonOptions = jsonOptions ?? DefaultJsonOptions;
        _eventTypes = BuildTypeMap(eventTypes);
    }

    /// <inheritdoc />
    public async Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (!_eventTypes.TryGetValue(message.EventType, out var eventType))
        {
            throw new InvalidOperationException(
                $"No CLR type registered for outbox EventType '{message.EventType}'. " +
                "Pass the event types to MessageTransportOutboxPublisher / AddMessageTransportOutboxPublisher.");
        }

        var deserialized = JsonSerializer.Deserialize(message.Payload, eventType, _jsonOptions)
            ?? throw new InvalidOperationException(
                $"Failed to deserialize outbox payload for EventType '{message.EventType}'.");

        if (deserialized is not IEvent @event)
        {
            throw new InvalidOperationException(
                $"Deserialized type '{eventType.FullName}' does not implement {nameof(IEvent)}.");
        }

        var metadata = TryReadMetadata(message.Metadata);
        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["outbox.streamId"] = message.StreamId,
            ["outbox.sequencePosition"] = message.SequencePosition.ToString(),
            ["outbox.eventType"] = message.EventType,
        };
        var context = new MessageContext
        {
            // Stable id for downstream IInboxStore deduplication.
            MessageId = message.Id.ToString(),
            CorrelationId = metadata?.CorrelationId,
            CausationId = metadata?.CausationId,
            TenantId = metadata?.TenantId,
            UserId = metadata?.UserId,
            EnqueuedAt = message.CreatedAt,
            Headers = MessageTrace.WithMetadataTraceHeaders(headers, metadata),
        };

        await _transport.PublishEventAsync(@event, context, cancellationToken).ConfigureAwait(false);
    }

    private EventMetadata? TryReadMetadata(string metadataJson)
    {
        if (string.IsNullOrWhiteSpace(metadataJson) || metadataJson is "{}")
            return null;

        try
        {
            return JsonSerializer.Deserialize<EventMetadata>(metadataJson, _jsonOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static IReadOnlyDictionary<string, Type> BuildTypeMap(IEnumerable<Type> eventTypes)
    {
        var map = new Dictionary<string, Type>(StringComparer.Ordinal);

        foreach (var type in eventTypes)
        {
            if (type is null)
                throw new ArgumentException("Event type list must not contain null entries.", nameof(eventTypes));
            if (!typeof(IEvent).IsAssignableFrom(type) || type.IsAbstract || type.IsInterface)
            {
                throw new ArgumentException(
                    $"Type '{type.FullName}' must be a concrete {nameof(IEvent)} implementation.",
                    nameof(eventTypes));
            }

            void Add(string? key)
            {
                if (string.IsNullOrWhiteSpace(key))
                    return;
                map[key] = type;
            }

            Add(type.FullName);
            Add(type.Name);
            Add(EventTypeNameResolver.GetName(type));
        }

        if (map.Count == 0)
            throw new ArgumentException("At least one event type is required.", nameof(eventTypes));

        return map;
    }
}
