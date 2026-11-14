namespace LawnDart.Outbox;

/// <summary>
/// Holds the outbox until a startup check has finished.
/// Created only when that check is registered. One <see cref="Hold"/>,
/// then <see cref="Release"/> after the check.
/// </summary>
internal sealed class OutboxSendBarrier
{
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Func<CancellationToken, Task>? _check;
    private int _holds;
    private int _ran;

    /// <summary>Adds one waiter the outbox must see before it publishes.</summary>
    public void Hold() => Interlocked.Increment(ref _holds);

    /// <summary>Stores the listener check. The outbox runs it after the host has started.</summary>
    public void SetCheck(Func<CancellationToken, Task> check) => _check = check;

    /// <summary>
    /// Runs the check once, then completes. Later callers wait for that same check.
    /// </summary>
    public async Task WaitAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _ran, 1) == 0)
        {
            try
            {
                if (_check is not null)
                    await _check(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                Release();
            }
        }

        await _ready.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private void Release()
    {
        if (Interlocked.Decrement(ref _holds) <= 0)
            _ready.TrySetResult();
    }
}
