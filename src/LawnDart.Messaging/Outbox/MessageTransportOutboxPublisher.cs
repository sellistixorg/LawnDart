using System.Text.Json;
using System.Text.Json.Serialization;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Outbox;

namespace LawnDart.Messaging.Outbox;

/// <summary>
/// Sample <see cref="IOutboxPublisher"/> that deserializes <see cref="OutboxMessage"/> payloads
/// and publishes them through <see cref="IMessageTransport"/> (InMemory in these packages).
/// </summary>
/// <remarks>
/// <para>
/// Delivery is <b>at-least-once</b>. This adapter does not claim exactly-once semantics —
/// register an <see cref="IInboxStore"/> (or equivalent) on consumers and use a stable
/// <see cref="MessageContext.MessageId"/> (this type uses the outbox row id).
/// </para>
/// <para>
/// Event type resolution uses the scoped <see cref="IEventTypeCatalog"/> with the
/// row's <see cref="OutboxMessage.SchemaVersion"/>. FullName and simple name remain
/// read aliases for older outbox rows. This publisher deserializes the stored
/// version's CLR type; upcast to the current type is not applied yet.
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
    private readonly IEventTypeCatalog _catalog;
    private readonly JsonSerializerOptions _jsonOptions;

    /// <summary>
    /// Creates a publisher that resolves event CLR types from <paramref name="catalog"/>.
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
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(catalog);

        _transport = transport;
        _catalog = catalog;
        _jsonOptions = jsonOptions ?? DefaultJsonOptions;
    }

    /// <summary>
    /// Creates a publisher that warms <paramref name="eventTypes"/> into the shared
    /// catalog and resolves through that catalog (not a private token map).
    /// </summary>
    public MessageTransportOutboxPublisher(
        IMessageTransport transport,
        IEnumerable<Type> eventTypes,
        JsonSerializerOptions? jsonOptions = null)
        : this(transport, EventTypeCatalog.Shared, jsonOptions)
    {
        ArgumentNullException.ThrowIfNull(eventTypes);
        RegisterKnownTypes(eventTypes);
    }

    /// <inheritdoc />
    public async Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        var schemaVersion = message.SchemaVersion <= 0 ? 1 : message.SchemaVersion;
        if (!_catalog.TryResolveType(message.EventType, schemaVersion, out var eventType))
        {
            throw new InvalidOperationException(
                $"No CLR type registered for outbox EventType '{message.EventType}' " +
                $"(SchemaVersion {schemaVersion}). " +
                "Pass event types to MessageTransportOutboxPublisher / AddMessageTransportOutboxPublisher " +
                "or register an IEventTypeCatalog.");
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

    private static void RegisterKnownTypes(IEnumerable<Type> eventTypes)
    {
        var sawType = false;
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

            sawType = true;
            if (EventTypeNameResolver.TryGetDeclaredName(type) is not null)
                EventTypeNameResolver.GetName(type);
        }

        if (!sawType)
            throw new ArgumentException("At least one event type is required.", nameof(eventTypes));
    }
}
