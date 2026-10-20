using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public class NewEventsSignalTests
{
    [Fact]
    public async Task Notify_WakesEveryWaiter_WithTheNewestPosition()
    {
        var signal = new NewEventsSignal();
        var first = signal.WaitAsync(TimeSpan.FromSeconds(30));
        var second = signal.WaitAsync(TimeSpan.FromSeconds(30));
        await WaitForWaitersAsync(signal, 2);

        signal.Notify(4);

        await first.WaitAsync(TimeSpan.FromSeconds(2));
        await second.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(4, signal.Position);
        Assert.Equal(1, signal.Generation);
    }

    [Fact]
    public async Task Notify_FromOutsideTheStore_WakesAWaiter()
    {
        var signal = new NewEventsSignal();
        var waiting = signal.WaitAsync(TimeSpan.FromSeconds(30));
        await WaitForWaitersAsync(signal, 1);

        // A later Postgres listener calls this same method. The store is not involved.
        signal.Notify(12);

        await waiting.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(12, signal.Position);
    }

    [Fact]
    public async Task RepeatedOrLowerPosition_StillWakes_AndDoesNotMoveBackward()
    {
        var signal = new NewEventsSignal();
        signal.Notify(8);

        var waiting = signal.WaitAsync(TimeSpan.FromSeconds(30));
        await WaitForWaitersAsync(signal, 1);
        signal.Notify(8);
        await waiting.WaitAsync(TimeSpan.FromSeconds(2));

        waiting = signal.WaitAsync(TimeSpan.FromSeconds(30));
        await WaitForWaitersAsync(signal, 1);
        signal.Notify(3);
        await waiting.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(8, signal.Position);
        Assert.Equal(3, signal.Generation);
    }

    [Fact]
    public void Notify_RejectsANegativePosition()
    {
        var signal = new NewEventsSignal();
        Assert.Throws<ArgumentOutOfRangeException>(() => signal.Notify(-1));
        Assert.Equal(0, signal.Generation);
    }

    [Fact]
    public async Task WaitAsync_RejectsANegativeTimeout()
    {
        var signal = new NewEventsSignal();
        // TimeSpan.FromMilliseconds(-1) is Timeout.InfiniteTimeSpan, which waits for a signal.
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => signal.WaitAsync(TimeSpan.FromMilliseconds(-2)));
    }

    [Fact]
    public async Task WaitAsync_ZeroTimeout_ReturnsWithoutASignal()
    {
        var signal = new NewEventsSignal();
        await signal.WaitAsync(TimeSpan.Zero);
        Assert.Equal(0, signal.Generation);
    }

    private static async Task WaitForWaitersAsync(NewEventsSignal signal, int count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (signal.WaitingCount < count)
            await Task.Delay(10, cts.Token);
    }
}
