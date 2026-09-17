using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Outbox;
using LawnDart.Serialization;

namespace LawnDart.Messaging.Outbox;

/// <summary>
/// Sample <see cref="IOutboxPublisher"/> that hydrates <see cref="OutboxMessage"/>
/// rows through <see cref="EventSession"/> and publishes them through
/// <see cref="IMessageTransport"/> (InMemory in these packages).
/// </summary>
/// <remarks>
/// <para>
/// Delivery is <b>at-least-once</b>. This adapter does not claim exactly-once semantics —
/// register an <see cref="IInboxStore"/> (or equivalent) on consumers and use a stable
/// <see cref="MessageContext.MessageId"/> (this type uses the outbox row id).
/// </para>
/// <para>
/// Hydrate uses the scoped catalog (token + <see cref="OutboxMessage.SchemaVersion"/>)
/// and the same upcast chain as typed store reads. There is no private deserialize
/// of the event payload.
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
    private readonly EventSession _session;
    private readonly JsonSerializerOptions _jsonOptions;

    /// <summary>
    /// Creates a publisher that hydrates through <paramref name="catalog"/>.
    /// </summary>
    /// <param name="transport">Transport used to publish (InMemory in these packages).</param>
    /// <param name="catalog">
    /// Scoped catalog. Resolve uses family token plus
    /// <see cref="OutboxMessage.SchemaVersion"/>.
    /// </param>
    /// <param name="jsonOptions">
    /// Optional JSON options. Defaults match SQL Server outbox payload serialization
    /// (<c>PropertyNameCaseInsensitive</c>, no camelCase rename).
    /// </param>
    public MessageTransportOutboxPublisher(
        IMessageTransport transport,
        IEventTypeCatalog catalog,
        JsonSerializerOptions? jsonOptions = null)
        : this(transport, catalog, upcastPipeline: null, jsonOptions)
    {
    }

    /// <summary>
    /// Creates a publisher that hydrates through <paramref name="catalog"/> and
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

        _jsonOptions = jsonOptions ?? DefaultJsonOptions;
        _transport = transport;
        _session = new EventSession(new OptionsEventSerializer(_jsonOptions), catalog, upcastPipeline);
    }

    /// <summary>
    /// Creates a publisher that materializes <paramref name="eventTypes"/> into
    /// an isolated catalog and hydrates through that catalog (not a private token map).
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

        var payload = Encoding.UTF8.GetBytes(message.Payload);
        var metadataBytes = string.IsNullOrWhiteSpace(message.Metadata)
            ? ReadOnlyMemory<byte>.Empty
            : Encoding.UTF8.GetBytes(message.Metadata);
        var recorded = new RecordedEvent(
            message.EventType,
            payload,
            message.StreamId,
            streamVersion: 0,
            sequencePosition: message.SequencePosition,
            commitTimestamp: message.CreatedAt,
            metadataBytes,
            message.SchemaVersion,
            string.IsNullOrWhiteSpace(message.ContentType)
                ? AppendEvent.DefaultContentType
                : message.ContentType);

        var sequenced = _session.Hydrate(recorded);
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

        public string ContentType => AppendEvent.DefaultContentType;

        public ReadOnlyMemory<byte> Serialize(object obj, Type type)
            => JsonSerializer.SerializeToUtf8Bytes(obj, type, _options);

        public object Deserialize(ReadOnlyMemory<byte> data, Type type)
            => JsonSerializer.Deserialize(data.Span, type, _options)
               ?? throw new InvalidOperationException($"Failed to deserialize {type.Name}");
    }
}
