namespace LawnDart.EventStore;

/// <summary>
/// Parses the standard three-part stream ID format used throughout the LawnDart event store.
/// </summary>
/// <remarks>
/// Stream IDs follow the format <c>{tenantId}:{aggregateType}:{aggregateId}</c> when multi-tenancy
/// is enabled, or the legacy two-part format <c>{aggregateType}:{aggregateId}</c> when it is not.
/// All event store backends and the projection engine must use this parser to avoid divergent
/// parsing logic.
/// </remarks>
public static class StreamIdParser
{
    /// <summary>
    /// Extracts the tenant ID from a stream ID.
    /// Returns <c>null</c> for two-part (non-tenant) stream IDs.
    /// </summary>
    public static string? ExtractTenantId(string streamId)
    {
        var parts = streamId.Split(':');
        return parts.Length >= 3 ? parts[0] : null;
    }

    /// <summary>
    /// Extracts the aggregate type from a stream ID.
    /// Handles both the three-part tenant format and the two-part legacy format.
    /// </summary>
    public static string ExtractAggregateType(string streamId)
    {
        var parts = streamId.Split(':');
        return parts.Length switch
        {
            >= 3 => parts[1], // {tenantId}:{aggregateType}:{aggregateId}
            2    => parts[0], // {aggregateType}:{aggregateId} (legacy)
            _    => streamId
        };
    }

    /// <summary>
    /// Extracts the aggregate ID segment from a stream ID.
    /// Returns <c>null</c> if the stream ID does not contain a recognisable aggregate ID part.
    /// </summary>
    public static string? ExtractAggregateIdString(string streamId)
    {
        var parts = streamId.Split(':');
        return parts.Length switch
        {
            >= 3 => parts[2], // {tenantId}:{aggregateType}:{aggregateId}
            2    => parts[1], // {aggregateType}:{aggregateId} (legacy)
            _    => null
        };
    }

    /// <summary>
    /// Extracts the aggregate ID as a <see cref="Guid"/> from a stream ID.
    /// Returns <c>null</c> if the ID segment is absent or cannot be parsed as a GUID.
    /// </summary>
    public static Guid? ExtractAggregateId(string streamId)
    {
        var raw = ExtractAggregateIdString(streamId);
        return raw != null && Guid.TryParse(raw, out var guid) ? guid : null;
    }
}
