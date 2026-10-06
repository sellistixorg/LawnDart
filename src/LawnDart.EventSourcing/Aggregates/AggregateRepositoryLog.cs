using Microsoft.Extensions.Logging;

namespace LawnDart.EventSourcing.Aggregates;

internal static partial class AggregateRepositoryLog
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Debug, Message = "Restored {Type} {StreamId} from snapshot at version {Version} (global seq {Seq})")]
    internal static partial void RestoredFromSnapshot(ILogger logger, string type, string streamId, long version, long seq);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "Snapshot restore failed for {Type} {StreamId}; falling back to full replay")]
    internal static partial void SnapshotRestoreFailed(ILogger logger, Exception exception, string type, string streamId);

    [LoggerMessage(EventId = 3, Level = LogLevel.Debug, Message = "Aggregate {Type} with stream {StreamId} not found")]
    internal static partial void AggregateNotFound(ILogger logger, string type, string streamId);

    [LoggerMessage(EventId = 4, Level = LogLevel.Debug, Message = "Loaded aggregate {Type} with stream {StreamId}, version {Version}, {DeltaEventCount} delta events (snapshot: {HasSnapshot})")]
    internal static partial void AggregateLoaded(ILogger logger, string type, string streamId, long version, int deltaEventCount, bool hasSnapshot);

    [LoggerMessage(EventId = 5, Level = LogLevel.Debug, Message = "Created new aggregate {Type} with StreamId: {StreamId}")]
    internal static partial void AggregateCreated(ILogger logger, string type, string streamId);

    [LoggerMessage(EventId = 6, Level = LogLevel.Debug, Message = "Found existing aggregate {Type} with stream {StreamId}, version {Version}")]
    internal static partial void ExistingAggregateFound(ILogger logger, string type, string streamId, long version);

    [LoggerMessage(EventId = 7, Level = LogLevel.Debug, Message = "Created new aggregate {Type} with stream {StreamId} (did not exist)")]
    internal static partial void AggregateCreatedBecauseMissing(ILogger logger, string type, string streamId);

    [LoggerMessage(EventId = 8, Level = LogLevel.Debug, Message = "No pending events to flush for aggregate {StreamId}")]
    internal static partial void NoPendingEvents(ILogger logger, string streamId);

    [LoggerMessage(EventId = 9, Level = LogLevel.Debug, Message = "Auto-set StreamId for aggregate {Type} with ID {Id}, StreamId: {StreamId}")]
    internal static partial void StreamIdAutoSet(ILogger logger, string type, Guid id, string streamId);

    [LoggerMessage(EventId = 10, Level = LogLevel.Debug, Message = "Saved {Count} events for aggregate {StreamId}, new version {Version}")]
    internal static partial void EventsSaved(ILogger logger, int count, string streamId, long version);

    [LoggerMessage(EventId = 11, Level = LogLevel.Warning, Message = "Authorization failed for command {CommandType}: {Reason}")]
    internal static partial void AuthorizationFailed(ILogger logger, string commandType, string? reason);

    [LoggerMessage(EventId = 12, Level = LogLevel.Debug, Message = "Command {CommandType} authorized for user {UserId}")]
    internal static partial void CommandAuthorized(ILogger logger, string commandType, string? userId);
}
