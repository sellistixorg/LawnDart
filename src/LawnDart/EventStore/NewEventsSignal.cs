namespace LawnDart.EventStore;

/// <summary>
/// In-process wake-up shared by readers of one event store.
/// </summary>
/// <remarks>
/// The store calls <see cref="Notify"/> after a save commits, with the newest
/// global position. A listener outside the store can call <see cref="Notify"/>
/// on the same instance. The signal carries no events. Readers still read
/// from their own checkpoint. A missed or extra call only changes when the
/// next read runs. The poll timer still delivers every committed event.
/// </remarks>
public sealed class NewEventsSignal
{
    private long _position;
    private long _generation;
    private int _waiting;
    private TaskCompletionSource<bool> _pulse = CreatePulse();

    /// <summary>
    /// Newest global position passed to <see cref="Notify"/> so far.
    /// Zero before the first call. A later call with a lower position does not move this backward.
    /// </summary>
    public long Position => Volatile.Read(ref _position);

    /// <summary>How many times <see cref="Notify"/> has run. Zero before the first call.</summary>
    internal long Generation => Volatile.Read(ref _generation);

    /// <summary>How many callers are inside <see cref="WaitAsync"/> right now.</summary>
    internal int WaitingCount => Volatile.Read(ref _waiting);

    /// <summary>
    /// Wakes every current waiter. Records <paramref name="globalPosition"/> when it is
    /// newer than <see cref="Position"/>.
    /// </summary>
    /// <param name="globalPosition">
    /// Newest committed global position. Zero is allowed and means the caller does not
    /// know a position. A negative value throws.
    /// </param>
    /// <remarks>
    /// Safe to call from any thread, including code that is not the event store.
    /// A position at or below <see cref="Position"/> still wakes waiters.
    /// </remarks>
    public void Notify(long globalPosition)
    {
        if (globalPosition < 0)
            throw new ArgumentOutOfRangeException(nameof(globalPosition), "Global position cannot be negative.");

        long observed;
        do
        {
            observed = Volatile.Read(ref _position);
            if (globalPosition <= observed)
                break;
        }
        while (Interlocked.CompareExchange(ref _position, globalPosition, observed) != observed);

        Interlocked.Increment(ref _generation);

        var next = CreatePulse();
        var previous = Interlocked.Exchange(ref _pulse, next);
        previous.TrySetResult(true);
    }

    /// <summary>
    /// Wakes waiters with the highest position in <paramref name="sequencePositions"/>.
    /// Does nothing when the list is empty.
    /// </summary>
    /// <param name="sequencePositions">Global positions assigned to one committed save.</param>
    internal void NotifyCommittedPositions(IReadOnlyList<long> sequencePositions)
    {
        ArgumentNullException.ThrowIfNull(sequencePositions);

        var newest = -1L;
        foreach (var position in sequencePositions)
        {
            if (position > newest)
                newest = position;
        }

        if (newest >= 0)
            Notify(newest);
    }

    /// <summary>
    /// Returns when <see cref="Notify"/> runs, or when <paramref name="timeout"/> elapses,
    /// whichever comes first.
    /// </summary>
    /// <param name="timeout">
    /// How long to wait for a signal. <see cref="TimeSpan.Zero"/> returns at once.
    /// <see cref="Timeout.InfiniteTimeSpan"/> waits until a signal or cancellation.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the wait. The call throws <see cref="OperationCanceledException"/> when this
    /// token is cancelled. A timeout returns without throwing.
    /// </param>
    public async Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        if (timeout < TimeSpan.Zero && timeout != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout cannot be negative.");

        cancellationToken.ThrowIfCancellationRequested();

        var pulse = Volatile.Read(ref _pulse).Task;
        if (pulse.IsCompleted || timeout == TimeSpan.Zero)
            return;

        Interlocked.Increment(ref _waiting);
        try
        {
            using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (timeout != Timeout.InfiniteTimeSpan)
                timeoutSource.CancelAfter(timeout);

            try
            {
                await pulse.WaitAsync(timeoutSource.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                // The poll interval elapsed. The caller reads again.
            }
        }
        finally
        {
            Interlocked.Decrement(ref _waiting);
        }
    }

    private static TaskCompletionSource<bool> CreatePulse() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
