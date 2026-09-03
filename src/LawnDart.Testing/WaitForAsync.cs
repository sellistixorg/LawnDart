namespace LawnDart.Testing;

/// <summary>
/// Polling utility for async test assertions.
/// Repeatedly evaluates a condition until it becomes true or the timeout elapses.
/// Useful when testing asynchronous side effects that complete slightly after an action.
/// </summary>
public static class WaitForAsync
{
    /// <summary>
    /// Polls <paramref name="condition"/> until it returns true or <paramref name="timeout"/> elapses.
    /// </summary>
    /// <param name="condition">The async condition to poll.</param>
    /// <param name="timeout">Maximum time to wait. Defaults to 5 seconds.</param>
    /// <param name="pollInterval">How frequently to poll. Defaults to 50 ms.</param>
    /// <param name="cancellationToken">External cancellation token.</param>
    /// <exception cref="TimeoutException">Thrown if the condition does not become true within the timeout.</exception>
    public static async Task UntilAsync(
        Func<Task<bool>> condition,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken cancellationToken = default)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(50);

        while (DateTime.UtcNow < deadline && !cancellationToken.IsCancellationRequested)
        {
            if (await condition())
                return;

            await Task.Delay(interval, cancellationToken);
        }

        if (cancellationToken.IsCancellationRequested)
            throw new OperationCanceledException(cancellationToken);

        throw new TimeoutException(
            $"Condition was not satisfied within {timeout ?? TimeSpan.FromSeconds(5)}.");
    }

    /// <summary>
    /// Polls a synchronous <paramref name="condition"/> until it returns true or <paramref name="timeout"/> elapses.
    /// </summary>
    public static Task UntilAsync(
        Func<bool> condition,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken cancellationToken = default)
        => UntilAsync(() => Task.FromResult(condition()), timeout, pollInterval, cancellationToken);

    /// <summary>
    /// Polls until <paramref name="getValue"/> returns a value satisfying <paramref name="predicate"/>,
    /// then returns that value. Throws <see cref="TimeoutException"/> if the deadline passes first.
    /// </summary>
    public static async Task<T> UntilAsync<T>(
        Func<T> getValue,
        Func<T, bool> predicate,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken cancellationToken = default)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(50);

        while (DateTime.UtcNow < deadline && !cancellationToken.IsCancellationRequested)
        {
            var value = getValue();
            if (predicate(value))
                return value;

            await Task.Delay(interval, cancellationToken);
        }

        if (cancellationToken.IsCancellationRequested)
            throw new OperationCanceledException(cancellationToken);

        throw new TimeoutException(
            $"Value did not satisfy predicate within {timeout ?? TimeSpan.FromSeconds(5)}.");
    }
}
