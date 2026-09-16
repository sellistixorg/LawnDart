using System.Text.Json;
using System.Text.Json.Serialization;
using LawnDart.Metadata;
using LawnDart.Serialization;

namespace LawnDart.EventStore;

/// <summary>
/// Typed session mapping: <see cref="IEvent"/> ↔ log frames.
/// </summary>
/// <remarks>
/// Serialize on the way in; token-only resolve and deserialize on the way out.
/// No upcast. Payload codec is <see cref="IEventSerializer"/>; metadata is UTF-8
/// JSON bytes (<c>application/json</c>), not the payload codec.
/// <para>
/// On hydrate, <see cref="EventMetadata.CommitTimestamp"/> and
/// <see cref="EventMetadata.SchemaVersion"/> are copied from the
/// <see cref="RecordedEvent"/> frame (frame is authority). Backends must not
/// resolve CLR types.
/// </para>
/// </remarks>
public sealed class EventSession
{
    private static readonly JsonSerializerOptions MetadataJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly IEventSerializer _serializer;
    private readonly IEventTypeCatalog _catalog;

    /// <param name="serializer">Payload codec for this session (one codec per session).</param>
    /// <param name="catalog">Token-only catalog. Defaults to <see cref="EventTypeCatalog.Shared"/>.</param>
    public EventSession(IEventSerializer serializer, IEventTypeCatalog? catalog = null)
    {
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _catalog = catalog ?? EventTypeCatalog.Shared;
    }

    /// <summary>
    /// Maps a typed event to an append envelope. Stamps <c>SchemaVersion = 1</c>
    /// for existing writers. Does not mutate <paramref name="metadata"/>.
    /// </summary>
    public AppendEvent ToAppendEvent(
        IEvent @event,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(@event);

        var type = @event.GetType();
        var token = _catalog.GetName(type);
        var payload = _serializer.Serialize(@event, type);
        var envelopeMetadata = SnapshotMetadata(metadata, @event, token);

        return new AppendEvent(
            token,
            payload,
            SerializeMetadata(envelopeMetadata),
            schemaVersion: 1,
            contentType: _serializer.ContentType,
            tags);
    }

    /// <summary>
    /// Hydrates a recorded frame to a typed <see cref="SequencedEvent"/>.
    /// Token-only resolve; no upcast. Content-type must match this session's
    /// codec; mismatch fails closed.
    /// </summary>
    public SequencedEvent Hydrate(RecordedEvent recorded)
    {
        ArgumentNullException.ThrowIfNull(recorded);

        if (!string.Equals(_serializer.ContentType, recorded.ContentType, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Event stored with ContentType '{recorded.ContentType}' but current serializer uses '{_serializer.ContentType}'.");
        }

        if (!_catalog.TryResolveType(recorded.EventType, out var type))
        {
            throw new InvalidOperationException($"Cannot resolve event type: {recorded.EventType}");
        }

        if (_serializer.Deserialize(recorded.Payload, type) is not IEvent @event)
        {
            throw new InvalidOperationException(
                $"Payload for '{recorded.EventType}' did not deserialize to {nameof(IEvent)} ({type.FullName}).");
        }

        return new SequencedEvent(
            @event,
            recorded.SequencePosition,
            recorded.StreamId,
            recorded.StreamVersion,
            HydrateMetadata(recorded),
            recorded.Tags);
    }

    private static EventMetadata SnapshotMetadata(EventMetadata? metadata, IEvent @event, string token)
    {
        if (metadata is null)
        {
            return new EventMetadata
            {
                EventId = @event.Id.ToString(),
                Timestamp = @event.Timestamp,
                SchemaName = token,
                SchemaVersion = 1
            };
        }

        var copy = CloneMetadata(metadata);
        if (string.IsNullOrWhiteSpace(copy.SchemaName))
            copy.SchemaName = token;
        copy.SchemaVersion = 1;
        return copy;
    }

    private static EventMetadata HydrateMetadata(RecordedEvent recorded)
    {
        var metadata = recorded.Metadata.IsEmpty
            ? new EventMetadata()
            : JsonSerializer.Deserialize<EventMetadata>(recorded.Metadata.Span, MetadataJsonOptions)
                ?? new EventMetadata();

        metadata.CommitTimestamp = recorded.CommitTimestamp;
        metadata.SchemaVersion = recorded.SchemaVersion;
        if (string.IsNullOrWhiteSpace(metadata.SchemaName))
            metadata.SchemaName = recorded.EventType;
        return metadata;
    }

    private static EventMetadata CloneMetadata(EventMetadata metadata)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(metadata, MetadataJsonOptions);
        return JsonSerializer.Deserialize<EventMetadata>(bytes, MetadataJsonOptions)
            ?? new EventMetadata();
    }

    private static ReadOnlyMemory<byte> SerializeMetadata(EventMetadata metadata)
        => JsonSerializer.SerializeToUtf8Bytes(metadata, MetadataJsonOptions);
}
