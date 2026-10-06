using Microsoft.Extensions.Logging;

namespace LawnDart.EventSourcing.SqlServer.Snapshots;

internal static partial class SqlServerSnapshotStoreLog
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "DCB snapshot checksum mismatch for {DcbId}; discarding")]
    internal static partial void DcbChecksumMismatch(ILogger logger, string dcbId);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "DCB snapshot deserialize failed for {DcbId}; discarding")]
    internal static partial void DcbDeserializeFailed(ILogger logger, Exception exception, string dcbId);

    [LoggerMessage(EventId = 3, Level = LogLevel.Warning, Message = "DCB snapshot load failed for {DcbId}; discarding")]
    internal static partial void DcbLoadFailed(ILogger logger, Exception exception, string dcbId);

    [LoggerMessage(EventId = 4, Level = LogLevel.Debug, Message = "Saved DCB snapshot for {DcbId} at global seq {Seq}")]
    internal static partial void DcbSaved(ILogger logger, string dcbId, long seq);

    [LoggerMessage(EventId = 5, Level = LogLevel.Warning, Message = "Stream snapshot checksum mismatch for {StreamId}; discarding")]
    internal static partial void StreamChecksumMismatch(ILogger logger, string streamId);

    [LoggerMessage(EventId = 6, Level = LogLevel.Warning, Message = "Stream snapshot deserialize failed for {StreamId}; discarding")]
    internal static partial void StreamDeserializeFailed(ILogger logger, Exception exception, string streamId);

    [LoggerMessage(EventId = 7, Level = LogLevel.Warning, Message = "Stream snapshot load failed for {StreamId}; discarding")]
    internal static partial void StreamLoadFailed(ILogger logger, Exception exception, string streamId);

    [LoggerMessage(EventId = 8, Level = LogLevel.Debug, Message = "Saved snapshot for {StreamId} at version {Version} (global seq {Seq})")]
    internal static partial void StreamSaved(ILogger logger, string streamId, long version, long seq);
}
