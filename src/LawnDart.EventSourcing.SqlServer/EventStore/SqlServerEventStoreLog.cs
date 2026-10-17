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

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Warning,
        Message = "Read horizon advanced past an unfinished write held for {HeldMilliseconds}ms by session {SessionId} (transaction {TransactionId}). Stable sequence {StableHead}, committed sequence {CommittedHead}.")]
    internal static partial void ReadHorizonAdvanced(
        ILogger logger,
        long heldMilliseconds,
        string sessionId,
        string transactionId,
        long stableHead,
        long committedHead);

    [LoggerMessage(
        EventId = 5,
        Level = LogLevel.Error,
        Message = "Committed event at sequence {SequencePosition} on stream {StreamId} landed below a read horizon that had already moved past it.")]
    internal static partial void ReadHorizonSkippedEvent(
        ILogger logger,
        long sequencePosition,
        string streamId);
}
