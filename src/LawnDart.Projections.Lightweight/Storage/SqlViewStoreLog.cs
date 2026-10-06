using Microsoft.Extensions.Logging;

namespace LawnDart.Projections.Storage;

internal static partial class SqlViewStoreLog
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Debug, Message = "Saved view to SQL: {ProjectionType}:{InstanceId} | ViewData size: {Size} bytes | Checkpoint: {Checkpoint}")]
    internal static partial void ViewSaved(ILogger logger, string projectionType, string instanceId, int size, long checkpoint);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "{ErrorType} (retry {RetryCount}/{MaxRetries}) for {ProjectionType}:{InstanceId}, retrying in {Delay}ms")]
    internal static partial void ViewSaveRetry(ILogger logger, string errorType, int retryCount, int maxRetries, string projectionType, string instanceId, double delay);

    [LoggerMessage(EventId = 3, Level = LogLevel.Error, Message = "SQL error saving view: {ProjectionType}:{InstanceId} | SQL Error {Number}: {Message} | State: {State} | Class: {Class} | Procedure: {Procedure} | LineNumber: {LineNumber} | ViewData size: {ViewDataSize} bytes | InstanceId length: {InstanceIdLength} | Connection string database: {ConnectionDatabase} | Retry count: {RetryCount}")]
    internal static partial void ViewSaveSqlFailed(
        ILogger logger,
        Exception exception,
        string projectionType,
        string instanceId,
        int number,
        string message,
        byte state,
        byte @class,
        string procedure,
        int lineNumber,
        int viewDataSize,
        int instanceIdLength,
        string connectionDatabase,
        int retryCount);

    [LoggerMessage(EventId = 4, Level = LogLevel.Error, Message = "Non-SQL error saving view: {ProjectionType}:{InstanceId} | Error Type: {ErrorType} | Message: {Message} | ViewData size: {ViewDataSize} bytes | InstanceId length: {InstanceIdLength} | Retry count: {RetryCount}")]
    internal static partial void ViewSaveFailed(ILogger logger, Exception exception, string projectionType, string instanceId, string? errorType, string message, int viewDataSize, int instanceIdLength, int retryCount);

    [LoggerMessage(EventId = 5, Level = LogLevel.Debug, Message = "Bulk-saved {Count} view(s) to SQL for {ProjectionType}")]
    internal static partial void ViewsSaved(ILogger logger, int count, string projectionType);

    [LoggerMessage(EventId = 6, Level = LogLevel.Warning, Message = "Bulk view save retry {RetryCount}/{MaxRetries} for {ProjectionType} ({Count} rows) in {Delay}ms: {Number}")]
    internal static partial void ViewsSaveRetry(ILogger logger, int retryCount, int maxRetries, string projectionType, int count, double delay, int number);

    [LoggerMessage(EventId = 7, Level = LogLevel.Error, Message = "Bulk view save failed for {ProjectionType} ({Count} rows) after {RetryCount} retries")]
    internal static partial void ViewsSaveFailed(ILogger logger, Exception exception, string projectionType, int count, int retryCount);

    [LoggerMessage(EventId = 8, Level = LogLevel.Debug, Message = "Retrieved view with checkpoint from SQL: {ProjectionType}:{InstanceId} | Checkpoint: {Checkpoint}")]
    internal static partial void ViewRetrieved(ILogger logger, string projectionType, string instanceId, long checkpoint);
}
