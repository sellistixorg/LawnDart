using Microsoft.Extensions.Logging;

namespace LawnDart.EventSourcing.Postgres.EventStore;

internal static partial class PostgresEventStoreLog
{
    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Warning,
        Message = "Transient Postgres error (attempt {Attempt}/{Max}, code {Code}): {Message}. Retrying in {DelayMs}ms.")]
    internal static partial void TransientError(ILogger logger, int attempt, int max, string code, string message, double delayMs);

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
    internal static partial void ReadHorizonSkippedEvent(ILogger logger, long sequencePosition, string streamId);

    [LoggerMessage(
        EventId = 6,
        Level = LogLevel.Error,
        Message = "Postgres subscription {SubscriberId} failed.")]
    internal static partial void SubscriptionFailed(ILogger logger, Exception exception, string subscriberId);
}
