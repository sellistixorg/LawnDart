using Microsoft.Extensions.Logging;

namespace LawnDart.EventSourcing.Dcb;

internal static partial class DcbRepositoryLog
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Debug, Message = "Restored DCB state {StateType} from snapshot at global seq {Seq}")]
    internal static partial void StateRestored(ILogger logger, string stateType, long seq);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "DCB snapshot restore failed for {DcbId}; falling back to full scan")]
    internal static partial void StateRestoreFailed(ILogger logger, Exception exception, string dcbId);

    [LoggerMessage(EventId = 3, Level = LogLevel.Debug, Message = "Rebuilt state {StateType} from {EventCount} delta events (snapshot: {HasSnapshot})")]
    internal static partial void StateRebuilt(ILogger logger, string stateType, int eventCount, bool hasSnapshot);

    [LoggerMessage(EventId = 4, Level = LogLevel.Debug, Message = "Appended {Count} DCB events with {TagCount} tags")]
    internal static partial void EventsAppended(ILogger logger, int count, int tagCount);

    [LoggerMessage(EventId = 5, Level = LogLevel.Debug, Message = "Created new DCB entity {EntityType} with {TagCount} tags")]
    internal static partial void EntityCreated(ILogger logger, string entityType, int tagCount);

    [LoggerMessage(EventId = 6, Level = LogLevel.Debug, Message = "Restored DCB entity {EntityType} from snapshot at global seq {Seq}")]
    internal static partial void EntityRestored(ILogger logger, string entityType, long seq);

    [LoggerMessage(EventId = 7, Level = LogLevel.Warning, Message = "DCB entity snapshot restore failed for {DcbId}; falling back to full scan")]
    internal static partial void EntityRestoreFailed(ILogger logger, Exception exception, string dcbId);

    [LoggerMessage(EventId = 8, Level = LogLevel.Debug, Message = "Loaded DCB entity {EntityType} with {EventCount} delta events (snapshot: {HasSnapshot})")]
    internal static partial void EntityLoaded(ILogger logger, string entityType, int eventCount, bool hasSnapshot);

    [LoggerMessage(EventId = 9, Level = LogLevel.Debug, Message = "Created new DCB entity {EntityType} (no existing events)")]
    internal static partial void EntityCreatedEmpty(ILogger logger, string entityType);

    [LoggerMessage(EventId = 10, Level = LogLevel.Debug, Message = "Handled command {CommandType} on DCB entity {EntityType}")]
    internal static partial void CommandHandled(ILogger logger, string commandType, string entityType);

    [LoggerMessage(EventId = 11, Level = LogLevel.Debug, Message = "Auto-injected tenant tag: tenant:{TenantId}")]
    internal static partial void TenantTagInjected(ILogger logger, string tenantId);
}
