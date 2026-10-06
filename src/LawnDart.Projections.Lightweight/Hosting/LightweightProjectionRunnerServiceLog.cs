using Microsoft.Extensions.Logging;
using LawnDart.Projections.Lightweight.Registration;

namespace LawnDart.Projections.Lightweight.Hosting;

internal static partial class LightweightProjectionRunnerServiceLog
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "[{Context}/{Projection}] Runner starting (kind={Kind}, node={Node}/{Total})")]
    internal static partial void RunnerStarting(ILogger logger, string context, string projection, ProjectionKind kind, int node, int total);

    [LoggerMessage(EventId = 2, Level = LogLevel.Information, Message = "[{Projection}] {Mode}: resuming from global position {Position}")]
    internal static partial void Resuming(ILogger logger, string projection, string mode, long position);

    [LoggerMessage(EventId = 3, Level = LogLevel.Information, Message = "[{Projection}] Lazy working-set mode: skipping bulk restore ({Count} will hydrate on demand)")]
    internal static partial void LazyWorkingSet(ILogger logger, string projection, int count);

    [LoggerMessage(EventId = 4, Level = LogLevel.Debug, Message = "[{Projection}] Store supports sequence check — idle polls will skip I/O when caught up")]
    internal static partial void SequenceCheckSupported(ILogger logger, string projection);

    [LoggerMessage(EventId = 5, Level = LogLevel.Debug, Message = "[{Projection}] Store does not support sequence check — falling back to timed poll interval")]
    internal static partial void SequenceCheckUnsupported(ILogger logger, string projection);

    [LoggerMessage(EventId = 6, Level = LogLevel.Debug, Message = "[{Projection}] Filtered catch-up: advancing cursor from {Prev} to {New} (head={Head}, windowEnd={WindowEnd}) — empty typed window")]
    internal static partial void FilteredCatchUp(ILogger logger, string projection, long prev, long @new, long head, long windowEnd);

    [LoggerMessage(EventId = 7, Level = LogLevel.Debug, Message = "[{Projection}] Sequence gap walk: advancing cursor from {Prev} to {New} (head={Head}, windowEnd={WindowEnd}) — empty global window")]
    internal static partial void SequenceGapWalk(ILogger logger, string projection, long prev, long @new, long head, long windowEnd);

    [LoggerMessage(EventId = 8, Level = LogLevel.Error, Message = "[{Projection}] Fail-closed event at or after sequence {Sequence}; backing off {Backoff}. Deploy the missing type or upcaster. There is no skip override.")]
    internal static partial void FailClosed(ILogger logger, Exception exception, string projection, long sequence, TimeSpan backoff);

    [LoggerMessage(EventId = 9, Level = LogLevel.Error, Message = "[{Projection}] Error in poll loop; backing off {Backoff}")]
    internal static partial void PollLoopError(ILogger logger, Exception exception, string projection, TimeSpan backoff);

    [LoggerMessage(EventId = 10, Level = LogLevel.Warning, Message = "[{Projection}] Final checkpoint flush did not complete cleanly")]
    internal static partial void FinalFlushIncomplete(ILogger logger, Exception exception, string projection);

    [LoggerMessage(EventId = 11, Level = LogLevel.Information, Message = "[{Projection}] Runner stopped at position {Position} ({Total} events total)")]
    internal static partial void RunnerStopped(ILogger logger, string projection, long position, long total);

    [LoggerMessage(EventId = 12, Level = LogLevel.Debug, Message = "[{Projection}] Hydrated instance '{Instance}' from durable store at seq {Seq}")]
    internal static partial void HydratedInstance(ILogger logger, string projection, string instance, long seq);

    [LoggerMessage(EventId = 13, Level = LogLevel.Debug, Message = "[{Projection}] New empty instance created for '{Instance}'")]
    internal static partial void NewEmptyInstance(ILogger logger, string projection, string instance);

    [LoggerMessage(EventId = 14, Level = LogLevel.Error, Message = "[{Projection}] Error processing event {EventType} for instance '{Instance}' (attempt {Attempt}/{Max}) at sequence {Sequence}")]
    internal static partial void EventProcessingFailed(ILogger logger, Exception exception, string projection, string eventType, string instance, int attempt, int max, long sequence);

    [LoggerMessage(EventId = 15, Level = LogLevel.Error, Message = "[{Projection}] Poison event halted runner after {Attempts} attempts. Event {EventType} at sequence {Sequence} instance '{Instance}'. Checkpoint will remain at the last successful sequence. Rebuild after a handler fix to recover.")]
    internal static partial void PoisonHalted(ILogger logger, Exception? exception, string projection, int attempts, string eventType, long sequence, string instance);

    [LoggerMessage(EventId = 16, Level = LogLevel.Debug, Message = "[{Projection}] Could not evaluate catch-up flush skip; allowing opportunistic flush")]
    internal static partial void CatchUpFlushSkipFailed(ILogger logger, Exception exception, string projection);

    [LoggerMessage(EventId = 17, Level = LogLevel.Debug, Message = "[{Projection}] Write-behind scheduled at position {Position} ({DirtyCount} view(s), inFlight={InFlight})")]
    internal static partial void WriteBehindScheduled(ILogger logger, string projection, long position, int dirtyCount, int inFlight);

    [LoggerMessage(EventId = 18, Level = LogLevel.Error, Message = "[{Projection}] Write-behind flush failed at position {Position}; dirties retained for retry")]
    internal static partial void WriteBehindFailed(ILogger logger, Exception exception, string projection, long position);

    [LoggerMessage(EventId = 19, Level = LogLevel.Debug, Message = "[{Projection}] Checkpoint saved at position {Position} ({DirtyCount} view(s))")]
    internal static partial void CheckpointSaved(ILogger logger, string projection, long position, int dirtyCount);

    [LoggerMessage(EventId = 20, Level = LogLevel.Debug, Message = "[{Projection}] Evicted clean instance '{Instance}' (working set {Count}/{Max})")]
    internal static partial void EvictedInstance(ILogger logger, string projection, string instance, int count, int max);

    [LoggerMessage(EventId = 21, Level = LogLevel.Information, Message = "[{Projection}] Evicted {Evicted} clean instance(s); working set now {Count} (max {Max})")]
    internal static partial void EvictedSummary(ILogger logger, string projection, int evicted, int count, int max);

    [LoggerMessage(EventId = 22, Level = LogLevel.Warning, Message = "[{Projection}] Far behind store head ({Lag}); poll recover from {Position}")]
    internal static partial void FarBehind(ILogger logger, string projection, long lag, long position);

    [LoggerMessage(EventId = 23, Level = LogLevel.Warning, Message = "[{Projection}] Subscription completed; poll recover")]
    internal static partial void SubscriptionCompleted(ILogger logger, string projection);

    [LoggerMessage(EventId = 24, Level = LogLevel.Warning, Message = "[{Projection}] Subscription fault; poll recover from {Position}")]
    internal static partial void SubscriptionFault(ILogger logger, Exception exception, string projection, long position);
}
