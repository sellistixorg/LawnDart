using Microsoft.Extensions.Logging;

namespace LawnDart.EventSourcing.Postgres.Snapshots;

internal static partial class PostgresSnapshotStoreLog
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "Snapshot checksum does not match for stream {StreamId}. The load returns empty and the repository replays the events.")]
    internal static partial void StreamChecksumMismatch(ILogger logger, string streamId);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "Snapshot checksum does not match for dynamic consistency boundary {DcbId}. The load returns empty and the repository replays the events.")]
    internal static partial void DcbChecksumMismatch(ILogger logger, string dcbId);

    [LoggerMessage(EventId = 3, Level = LogLevel.Warning, Message = "Snapshot for stream {StreamId} could not be read. The load returns empty and the repository replays the events.")]
    internal static partial void StreamDeserializeFailed(ILogger logger, Exception exception, string streamId);

    [LoggerMessage(EventId = 4, Level = LogLevel.Warning, Message = "Snapshot for dynamic consistency boundary {DcbId} could not be read. The load returns empty and the repository replays the events.")]
    internal static partial void DcbDeserializeFailed(ILogger logger, Exception exception, string dcbId);

    [LoggerMessage(EventId = 5, Level = LogLevel.Warning, Message = "Snapshot load failed for stream {StreamId}. The load returns empty and the repository replays the events.")]
    internal static partial void StreamLoadFailed(ILogger logger, Exception exception, string streamId);

    [LoggerMessage(EventId = 6, Level = LogLevel.Warning, Message = "Snapshot load failed for dynamic consistency boundary {DcbId}. The load returns empty and the repository replays the events.")]
    internal static partial void DcbLoadFailed(ILogger logger, Exception exception, string dcbId);

    [LoggerMessage(EventId = 7, Level = LogLevel.Debug, Message = "Saved snapshot for stream {StreamId} at version {Version}, global position {GlobalPosition}.")]
    internal static partial void StreamSaved(ILogger logger, string streamId, long version, long globalPosition);

    [LoggerMessage(EventId = 8, Level = LogLevel.Debug, Message = "Saved snapshot for dynamic consistency boundary {DcbId} at global position {GlobalPosition}.")]
    internal static partial void DcbSaved(ILogger logger, string dcbId, long globalPosition);
}
