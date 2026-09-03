namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// Pure helpers for Lightweight flush / catch-up decisions.
/// </summary>
public static class ProjectionFlushPolicy
{
    /// <summary>
    /// Returns whether an opportunistic (tail / gap-walk) flush should be skipped because the
    /// runner is still catching up past <paramref name="catchUpFlushThreshold"/>.
    /// </summary>
    public static bool ShouldSkipOpportunisticFlush(
        bool skipTailFlushWhileCatchingUp,
        bool supportsSequenceCheck,
        long eventStoreHead,
        long lastProcessedPosition,
        long catchUpFlushThreshold)
    {
        if (!skipTailFlushWhileCatchingUp || !supportsSequenceCheck)
            return false;

        return eventStoreHead - lastProcessedPosition > catchUpFlushThreshold;
    }
}
