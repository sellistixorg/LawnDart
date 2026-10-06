using Microsoft.Extensions.Logging;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

internal static partial class SqlServerEventStoreLog
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "Transient SQL error (attempt {Attempt}/{Max}, code {Code}): {Message}. Retrying in {DelayMs}ms.")]
    internal static partial void TransientSqlError(ILogger logger, int attempt, int max, int code, string message, double delayMs);

    [LoggerMessage(EventId = 2, Level = LogLevel.Debug, Message = "Wrote {Count} events to outbox for stream {StreamId}")]
    internal static partial void OutboxEventsWritten(ILogger logger, int count, string streamId);

    [LoggerMessage(EventId = 3, Level = LogLevel.Error, Message = "SQL Server subscription {SubscriberId} failed.")]
    internal static partial void SubscriptionFailed(ILogger logger, Exception exception, string subscriberId);
}
