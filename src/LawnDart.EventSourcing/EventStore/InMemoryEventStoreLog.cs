using Microsoft.Extensions.Logging;

namespace LawnDart.EventSourcing.EventStore;

internal static partial class InMemoryEventStoreLog
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Error, Message = "InMemory subscription {SubscriberId} failed.")]
    internal static partial void SubscriptionFailed(ILogger logger, Exception exception, string subscriberId);

    [LoggerMessage(EventId = 2, Level = LogLevel.Debug, Message = "Appended {Count} events to stream {StreamId}, versions {FromVersion}-{ToVersion}")]
    internal static partial void StreamEventsAppended(ILogger logger, int count, string streamId, long fromVersion, long toVersion);

    [LoggerMessage(EventId = 3, Level = LogLevel.Debug, Message = "Appended {Count} events with DCB condition, sequence positions {FromPosition}-{ToPosition}")]
    internal static partial void DcbEventsAppended(ILogger logger, int count, long fromPosition, long toPosition);
}
