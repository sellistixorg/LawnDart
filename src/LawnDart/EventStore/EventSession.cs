using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using LawnDart.Metadata;
using LawnDart.Serialization;

namespace LawnDart.EventStore;

/// <summary>
/// Typed session mapping: <see cref="IEvent"/> to stored records.
/// </summary>
/// <remarks>
/// Serialize on the way in; resolve <c>(token, SchemaVersion)</c>, deserialize
/// the stored version's .NET type, then upcast to current. The payload format is
/// <see cref="IEventSerializer"/>. Metadata is UTF-8 JSON bytes
/// (<c>application/json</c>), separate from the payload format.
/// <para>
/// When the session reads a record, <see cref="EventMetadata.CommitTimestamp"/> and
/// <see cref="EventMetadata.SchemaVersion"/> are copied from the
/// <see cref="RecordedEvent"/>. The stored record is authority. Backends must not
/// resolve .NET types.
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
    private readonly EventUpcastPipeline? _upcast;

    /// <param name="serializer">Payload format for this session (one format per session).</param>
    /// <param name="catalog">Scoped catalog from <c>WithEventTypes</c> / <see cref="EventTypeCatalog.Materialize"/>.</param>
    /// <param name="upcastPipeline">
    /// Chain from historical types to current. Null still throws when the
    /// stored type is not this process's current type.
    /// </param>
    public EventSession(
        IEventSerializer serializer,
        IEventTypeCatalog catalog,
        EventUpcastPipeline? upcastPipeline = null)
    {
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _upcast = upcastPipeline;
    }

    /// <summary>Scoped catalog this session resolves against.</summary>
    internal IEventTypeCatalog Catalog => _catalog;

    /// <summary>
    /// Maps a typed event to an append envelope. Stamps frame
    /// <see cref="AppendEvent.SchemaVersion"/> from the current type and mirrors
    /// it onto <see cref="EventMetadata.SchemaVersion"/> (overwrites a caller
    /// integer). Rejects a historical .NET type for an event name this catalog
    /// already knows. Does not mutate <paramref name="metadata"/>.
    /// </summary>
    public AppendEvent ToAppendEvent(
        IEvent @event,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(@event);

        var type = @event.GetType();
        var token = _catalog.GetName(type);
        if (_catalog.TryResolveType(token, out var current) && current != type)
        {
            throw new InvalidOperationException(
                $"Cannot append historical event type '{type.FullName}' for family '{token}'. " +
                $"This process appends only the current type '{current.FullName}'.");
        }

        var schemaVersion = ReadDeclaredSchemaVersion(type);
        var payload = _serializer.Serialize(@event, type);
        var envelopeMetadata = SnapshotMetadata(metadata, @event, token, schemaVersion);

        return new AppendEvent(
            token,
            payload,
            SerializeMetadata(envelopeMetadata),
            schemaVersion,
            codecId: EventCodec.IdFor(_serializer.ContentType),
            tags);
    }

    /// <summary>
    /// Reads a stored record into a typed <see cref="SequencedEvent"/>.
    /// Resolve is the event name plus <see cref="RecordedEvent.SchemaVersion"/>;
    /// deserialize that .NET type, then upcast to current.
    /// Those failures throw <see cref="EventHydrationException"/>.
    /// Log and raw copy paths do not use this method.
    /// </summary>
    /// <exception cref="EventContentTypeMismatchException">The stored payload-format number is not this session's format, or the record is <see cref="EventCodec.Opaque"/>.</exception>
    /// <exception cref="EventSchemaTooNewException">Stored version is newer than this process's current type.</exception>
    /// <exception cref="UnknownEventFamilyException">The event name is not in this process's catalog.</exception>
    /// <exception cref="EventSchemaNotInCatalogException">Known event name, but this version's .NET type is not registered.</exception>
    /// <exception cref="EventPayloadException">Payload bytes did not deserialize, or an upcaster threw.</exception>
    /// <exception cref="MissingEventUpcasterException">Historical type is in the catalog but no chain reaches current.</exception>
    public SequencedEvent Hydrate(RecordedEvent recorded)
    {
        ArgumentNullException.ThrowIfNull(recorded);

        var sessionCodecId = EventCodec.IdFor(_serializer.ContentType);
        if (recorded.CodecId == EventCodec.Opaque || recorded.CodecId != sessionCodecId)
        {
            throw new EventContentTypeMismatchException(recorded.CodecId, sessionCodecId);
        }

        var schemaVersion = recorded.SchemaVersion <= 0 ? 1 : recorded.SchemaVersion;
        var token = recorded.EventType;
        if (!_catalog.TryResolveType(token, schemaVersion, out var type))
            throw UnresolvedHydration(token, schemaVersion);

        var declared = ReadDeclaredSchemaVersion(type);
        if (schemaVersion > declared
            && _catalog.TryResolveType(_catalog.GetName(type), out var current))
        {
            var currentVersion = ReadDeclaredSchemaVersion(current);
            if (schemaVersion > currentVersion)
                throw new EventSchemaTooNewException(_catalog.GetName(type), schemaVersion, currentVersion);
        }

        object deserialized;
        try
        {
            deserialized = _serializer.Deserialize(recorded.Payload, type);
        }
        catch (Exception ex) when (ex is not EventHydrationException and not OperationCanceledException)
        {
            throw new EventPayloadException(
                token,
                schemaVersion,
                $"Payload for '{token}' (SchemaVersion {schemaVersion}) could not be deserialized as {type.FullName}.",
                ex);
        }

        if (deserialized is not IEvent @event)
        {
            throw new EventPayloadException(
                token,
                schemaVersion,
                $"Payload for '{token}' (SchemaVersion {schemaVersion}) did not deserialize to {nameof(IEvent)} ({type.FullName}).");
        }

        @event = UpcastToCurrent(@event, token, schemaVersion);

        return new SequencedEvent(
            @event,
            recorded.SequencePosition,
            recorded.StreamId,
            recorded.StreamVersion,
            HydrateMetadata(recorded),
            recorded.Tags);
    }

    private static EventMetadata SnapshotMetadata(
        EventMetadata? metadata,
        IEvent @event,
        string token,
        int schemaVersion)
    {
        if (metadata is null)
        {
            return new EventMetadata
            {
                EventId = @event.Id.ToString(),
                Timestamp = @event.Timestamp,
                SchemaName = token,
                SchemaVersion = schemaVersion
            };
        }

        var copy = CloneMetadata(metadata);
        if (string.IsNullOrWhiteSpace(copy.SchemaName))
            copy.SchemaName = token;
        copy.SchemaVersion = schemaVersion;
        // One metadata object is often reused across appends. The event id is
        // the event's own id, so those appends do not collide.
        copy.EventId = @event.Id.ToString();
        return copy;
    }

    private static EventMetadata HydrateMetadata(RecordedEvent recorded)
    {
        var metadata = recorded.Metadata.IsEmpty
            ? new EventMetadata()
            : JsonSerializer.Deserialize<EventMetadata>(recorded.Metadata.Span, MetadataJsonOptions)
                ?? new EventMetadata();

        metadata.CommitTimestamp = recorded.CommitTimestamp;
        metadata.SchemaVersion = recorded.SchemaVersion <= 0 ? 1 : recorded.SchemaVersion;
        if (string.IsNullOrWhiteSpace(metadata.SchemaName))
            metadata.SchemaName = recorded.EventType;
        return metadata;
    }

    private IEvent UpcastToCurrent(IEvent stored, string token, int schemaVersion)
    {
        if (!_catalog.TryResolveType(token, out var current) || current == stored.GetType())
            return stored;

        if (_upcast is null)
        {
            throw new MissingEventUpcasterException(
                token,
                schemaVersion,
                ReadDeclaredSchemaVersion(current));
        }

        try
        {
            return _upcast.UpcastToCurrent(stored);
        }
        catch (EventHydrationException)
        {
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new EventPayloadException(
                token,
                schemaVersion,
                $"Upcast failed for '{token}' (SchemaVersion {schemaVersion}).",
                ex);
        }
    }

    private EventHydrationException UnresolvedHydration(string token, int schemaVersion)
    {
        if (_catalog.TryResolveType(token, out var current))
        {
            var currentVersion = ReadDeclaredSchemaVersion(current);
            if (schemaVersion > currentVersion)
                return new EventSchemaTooNewException(token, schemaVersion, currentVersion);

            return new EventSchemaNotInCatalogException(token, schemaVersion);
        }

        return new UnknownEventFamilyException(token);
    }

    private static int ReadDeclaredSchemaVersion(Type type)
    {
        var attr = type.GetCustomAttribute<EventTypeNameAttribute>(inherit: false);
        return attr is { Version: >= 1 } ? attr.Version : 1;
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
