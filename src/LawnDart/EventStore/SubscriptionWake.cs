namespace LawnDart.EventStore;

/// <summary>
/// Chooses how long a subscription waits after a read that found nothing new.
/// </summary>
internal static class SubscriptionWake
{
    /// <summary>
    /// How long to wait before reading again when a wake-up named a position
    /// that an open transaction still hides. This is not the idle poll.
    /// </summary>
    internal static readonly TimeSpan HiddenPositionRetry = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// Returns the idle poll, or a short retry when <paramref name="horizonHeld"/>
    /// is true and <paramref name="signaledPosition"/> is still ahead of
    /// <paramref name="visibleHead"/>.
    /// </summary>
    /// <remarks>
    /// A wake-up records the committed position and ends the current wait.
    /// The following read can still miss that row while another transaction
    /// holds the gap-free head. The wake-up is then already consumed, and the
    /// transaction that clears the head does not signal again. The short retry
    /// reads until the position is visible. A quiet log keeps the idle poll.
    /// </remarks>
    internal static TimeSpan IdleWait(TimeSpan idle, long visibleHead, long signaledPosition, bool horizonHeld)
    {
        if (horizonHeld && signaledPosition > visibleHead)
            return HiddenPositionRetry;
        return idle;
    }
}
