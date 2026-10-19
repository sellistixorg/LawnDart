using System.Text;
using System.Text.Json;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Outbox;
using LawnDart.Serialization;

namespace LawnDart.Messaging.Outbox;

/// <summary>
/// Sample <see cref="IOutboxPublisher"/> that reads <see cref="OutboxMessage"/>
/// rows into objects through <see cref="EventSession"/> and publishes them through
/// <see cref="IMessageTransport"/>.
/// The shipped transports are the in-process, RabbitMQ, and Azure Service Bus implementations.
/// </summary>
/// <remarks>
/// <para>
/// Delivery is <b>at-least-once</b>. This adapter does not claim exactly-once semantics.
/// Register an <see cref="IInboxStore"/> (or equivalent) on consumers and use a stable
/// <see cref="MessageContext.MessageId"/> (this type uses the outbox row id).
/// </para>
/// <para>
/// The read uses the scoped catalog (event name plus <see cref="OutboxMessage.SchemaVersion"/>)
/// and the same upcast chain as typed store reads. There is no private deserialize
/// of the event payload.
/// </para>
/// </remarks>
public sealed class MessageTransportOutboxPublisher : IOutboxPublisher
{
    private readonly IMessageTransport _transport;
    private readonly EventSession _session;
    private readonly JsonSerializerOptions _jsonOptions;

    /// <summary>
    /// Creates a publisher that reads rows into objects through <paramref name="catalog"/>.
    /// </summary>
    /// <param name="transport">
    /// Transport used to publish. The shipped transports are the in-process,
    /// RabbitMQ, and Azure Service Bus implementations.
    /// </param>
    /// <param name="catalog">
    /// Scoped catalog. Resolve uses the event name plus
    /// <see cref="OutboxMessage.SchemaVersion"/>.
    /// </param>
    /// <param name="jsonOptions">
    /// Optional JSON options. The default is <see cref="MessagingJson.Options"/>.
    /// </param>
    public MessageTransportOutboxPublisher(
        IMessageTransport transport,
        IEventTypeCatalog catalog,
        JsonSerializerOptions? jsonOptions = null)
        : this(transport, catalog, upcastPipeline: null, jsonOptions)
    {
    }

    /// <summary>
    /// Creates a publisher that reads rows into objects through <paramref name="catalog"/> and
    /// upcasts historical rows to the current type.
    /// </summary>
    public MessageTransportOutboxPublisher(
        IMessageTransport transport,
        IEventTypeCatalog catalog,
        EventUpcastPipeline? upcastPipeline,
        JsonSerializerOptions? jsonOptions = null)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(catalog);

        _jsonOptions = jsonOptions ?? MessagingJson.Options;
        _transport = transport;
        _session = new EventSession(new OptionsEventSerializer(_jsonOptions), catalog, upcastPipeline);
    }

    /// <summary>
    /// Creates a publisher that materializes <paramref name="eventTypes"/> into
    /// an isolated catalog and reads rows into objects through that catalog (not a private name map).
    /// </summary>
    public MessageTransportOutboxPublisher(
        IMessageTransport transport,
        IEnumerable<Type> eventTypes,
        JsonSerializerOptions? jsonOptions = null)
        : this(transport, MaterializeRequired(eventTypes), upcastPipeline: null, jsonOptions)
    {
    }

    /// <inheritdoc />
    public async Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        var metadataBytes = string.IsNullOrWhiteSpace(message.Metadata)
            ? ReadOnlyMemory<byte>.Empty
            : Encoding.UTF8.GetBytes(message.Metadata);
        var recorded = new RecordedEvent(
            message.EventType,
            message.Payload,
            message.StreamId,
            streamVersion: 0,
            sequencePosition: message.SequencePosition,
            commitTimestamp: message.CreatedAt,
            metadataBytes,
            message.SchemaVersion,
            message.CodecId);

        var sequenced = _session.Hydrate(recorded);
        var metadata = TryReadMetadata(message.Metadata);
        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [MessageHeaders.OutboxStreamId] = message.StreamId,
            [MessageHeaders.OutboxSequencePosition] = message.SequencePosition.ToString(),
            [MessageHeaders.OutboxEventType] = message.EventType,
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

        await _transport.PublishEventAsync(sequenced.Event, context, cancellationToken).ConfigureAwait(false);
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

    private static EventTypeCatalog MaterializeRequired(IEnumerable<Type> eventTypes)
    {
        ArgumentNullException.ThrowIfNull(eventTypes);
        var list = eventTypes as IReadOnlyCollection<Type> ?? eventTypes.ToArray();
        if (list.Count == 0)
            throw new ArgumentException("At least one event type is required.", nameof(eventTypes));
        return EventTypeCatalog.Materialize(list);
    }

    private sealed class OptionsEventSerializer : IEventSerializer
    {
        private readonly JsonSerializerOptions _options;

        public OptionsEventSerializer(JsonSerializerOptions options) => _options = options;

        public string ContentType => EventCodec.JsonMime;

        public ReadOnlyMemory<byte> Serialize(object obj, Type type)
            => JsonSerializer.SerializeToUtf8Bytes(obj, type, _options);

        public object Deserialize(ReadOnlyMemory<byte> data, Type type)
            => JsonSerializer.Deserialize(data.Span, type, _options)
               ?? throw new InvalidOperationException($"Failed to deserialize {type.Name}");
    }
}
