using System.Globalization;
using System.Text.Json;

namespace LawnDart.EventStore;

/// <summary>
/// LawnDart's <see cref="IRawEvent"/>: a recorded log frame viewed as an
/// <see cref="IEvent"/>. Not a typed-hydrate fallback.
/// </summary>
/// <remarks>
/// A process without the CLR event type appends and copies through
/// <see cref="IEventLog"/>. <see cref="EventSession.Hydrate"/> still fails
/// closed when the family token is unknown. This type exists so
/// <see cref="IRawEvent"/> has a production implementation and so typed
/// helpers such as <see cref="EventQueryMatcher.Matches(SequencedEvent, Query, IEventTypeCatalog)"/>
/// can read the stored family token instead of this CLR name.
/// <para>
/// <see cref="Id"/> and <see cref="Timestamp"/> come from the caller or from
/// metadata JSON when present. This type never mints <c>Guid.NewGuid()</c> or
/// <c>DateTime.UtcNow</c>.
/// </para>
/// </remarks>
public sealed class RawRecordedEvent : IRawEvent
{
    /// <param name="recorded">Frame to wrap. Payload is snapshotted for <see cref="RawPayload"/>.</param>
    /// <param name="id">Optional event id. Default is <see cref="Guid.Empty"/> (not minted).</param>
    /// <param name="timestamp">Optional business time. Default is <see cref="DateTime.MinValue"/> (not minted).</param>
    public RawRecordedEvent(RecordedEvent recorded, Guid id = default, DateTime timestamp = default)
    {
        Recorded = recorded ?? throw new ArgumentNullException(nameof(recorded));
        RawPayload = recorded.Payload.ToArray();
        Id = id;
        Timestamp = timestamp;
    }

    /// <summary>
    /// Wraps <paramref name="recorded"/>. Reads <see cref="Id"/> /
    /// <see cref="Timestamp"/> from metadata JSON when those fields are present.
    /// Missing fields stay default.
    /// </summary>
    public static RawRecordedEvent From(RecordedEvent recorded)
    {
        ArgumentNullException.ThrowIfNull(recorded);
        TryReadIdentity(recorded.Metadata.Span, out var id, out var timestamp);
        return new RawRecordedEvent(recorded, id, timestamp);
    }

    /// <summary>The wrapped log frame.</summary>
    public RecordedEvent Recorded { get; }

    /// <inheritdoc />
    public string TypeName => Recorded.EventType;

    /// <inheritdoc />
    public byte[] RawPayload { get; }

    /// <inheritdoc />
    public Guid Id { get; }

    /// <inheritdoc />
    public DateTime Timestamp { get; }

    private static void TryReadIdentity(ReadOnlySpan<byte> metadataJson, out Guid id, out DateTime timestamp)
    {
        id = default;
        timestamp = default;
        if (metadataJson.IsEmpty)
            return;

        try
        {
            using var doc = JsonDocument.Parse(metadataJson.ToArray());
            var root = doc.RootElement;
            if (TryGetProperty(root, "EventId", "eventId", out var idEl)
                && idEl.ValueKind == JsonValueKind.String
                && Guid.TryParse(idEl.GetString(), out var parsed)
                && parsed != Guid.Empty)
            {
                id = parsed;
            }

            if (TryGetProperty(root, "Timestamp", "timestamp", out var tsEl)
                && tsEl.ValueKind == JsonValueKind.String
                && DateTime.TryParse(
                    tsEl.GetString(),
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.RoundtripKind,
                    out var dt))
            {
                timestamp = dt.Kind == DateTimeKind.Unspecified
                    ? DateTime.SpecifyKind(dt, DateTimeKind.Utc)
                    : dt.ToUniversalTime();
            }
        }
        catch (JsonException)
        {
            // Leave defaults — do not mint.
        }
    }

    private static bool TryGetProperty(JsonElement root, string pascal, string camel, out JsonElement value)
    {
        if (root.TryGetProperty(pascal, out value))
            return true;
        return root.TryGetProperty(camel, out value);
    }
}
