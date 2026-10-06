using Microsoft.Extensions.Logging;

namespace LawnDart.Projections.Checkpoints;

internal static partial class SqlCheckpointStoreLog
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Debug, Message = "Saved checkpoint for {ProjectionType} on Node {NodeId} at position {Position}")]
    internal static partial void CheckpointSaved(ILogger logger, string projectionType, int nodeId, long position);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "Connection pool timeout (retry {RetryCount}/{MaxRetries}) for checkpoint {ProjectionType}:Node{NodeId}, retrying in {Delay}ms")]
    internal static partial void CheckpointSaveRetry(ILogger logger, int retryCount, int maxRetries, string projectionType, int nodeId, double delay);

    [LoggerMessage(EventId = 3, Level = LogLevel.Error, Message = "Error saving checkpoint for {ProjectionType} on Node {NodeId} | Error Type: {ErrorType} | Message: {Message} | Retry count: {RetryCount}")]
    internal static partial void CheckpointSaveFailed(ILogger logger, Exception exception, string projectionType, int nodeId, string? errorType, string message, int retryCount);

    [LoggerMessage(EventId = 4, Level = LogLevel.Debug, Message = "Retrieved stream checkpoint from view: {ProjectionType}:{StreamId} | Checkpoint: {Checkpoint}")]
    internal static partial void StreamCheckpointRetrieved(ILogger logger, string projectionType, string streamId, long checkpoint);

    [LoggerMessage(EventId = 5, Level = LogLevel.Trace, Message = "SaveStreamCheckpointAsync called for {ProjectionType}:{StreamId} at position {Position} (no-op, checkpoint saved with view)")]
    internal static partial void StreamCheckpointSaveNoOp(ILogger logger, string projectionType, string streamId, long position);
}
