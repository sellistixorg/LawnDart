namespace LawnDart.EventStore;

/// <summary>
/// Reads the parts of canonical stream id text that a store keeps as a string.
/// Uses the <see cref="StreamId"/> rules, so every store splits the text the same way.
/// </summary>
internal static class StreamIdText
{
    internal static string? TenantId(string streamId)
        => StreamId.TryParse(streamId, out var parsed) ? parsed.TenantId : null;

    internal static string AggregateType(string streamId)
        => StreamId.TryParse(streamId, out var parsed) ? parsed.Type : streamId;

    internal static string? AggregateIdString(string streamId)
        => StreamId.TryParse(streamId, out var parsed) && !parsed.IsDcb ? parsed.Id : null;

    internal static Guid? AggregateId(string streamId)
        => AggregateIdString(streamId) is { } raw && Guid.TryParse(raw, out var guid) ? guid : null;
}
