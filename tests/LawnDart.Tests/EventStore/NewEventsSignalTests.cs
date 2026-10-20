using System.Diagnostics;
using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public class NewEventsSignalTests
{
    [Fact]
    public async Task Notify_WakesEveryWaiter_WithTheNewestPosition()
    {
        var signal = new NewEventsSignal();
        var ticket = signal.TakeTicket();
        var first = signal.WaitAsync(ticket, TimeSpan.FromSeconds(30));
        var second = signal.WaitAsync(ticket, TimeSpan.FromSeconds(30));
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
        var ticket = signal.TakeTicket();
        var waiting = signal.WaitAsync(ticket, TimeSpan.FromSeconds(30));
        await WaitForWaitersAsync(signal, 1);

        // A later Postgres listener calls this same method. The store is not involved.
        signal.Notify(12);

        await waiting.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(12, signal.Position);
    }

    [Fact]
    public async Task NotifyBetweenTicketAndWait_ReturnsBeforeThePollInterval()
    {
        var signal = new NewEventsSignal();
        var ticket = signal.TakeTicket();
        signal.Notify(1);

        var started = Stopwatch.StartNew();
        await signal.WaitAsync(ticket, TimeSpan.FromSeconds(30));

        Assert.True(started.Elapsed < TimeSpan.FromSeconds(2), $"waited {started.Elapsed}");
        Assert.Equal(1, signal.Position);
    }

    [Fact]
    public async Task RepeatedOrLowerPosition_StillWakes_AndDoesNotMoveBackward()
    {
        var signal = new NewEventsSignal();
        signal.Notify(8);

        var ticket = signal.TakeTicket();
        var waiting = signal.WaitAsync(ticket, TimeSpan.FromSeconds(30));
        await WaitForWaitersAsync(signal, 1);
        signal.Notify(8);
        await waiting.WaitAsync(TimeSpan.FromSeconds(2));

        ticket = signal.TakeTicket();
        waiting = signal.WaitAsync(ticket, TimeSpan.FromSeconds(30));
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
        var ticket = signal.TakeTicket();
        // TimeSpan.FromMilliseconds(-1) is Timeout.InfiniteTimeSpan, which waits for a signal.
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => signal.WaitAsync(ticket, TimeSpan.FromMilliseconds(-2)));
    }

    [Fact]
    public async Task WaitAsync_ZeroTimeout_ReturnsWithoutASignal()
    {
        var signal = new NewEventsSignal();
        await signal.WaitAsync(signal.TakeTicket(), TimeSpan.Zero);
        Assert.Equal(0, signal.Generation);
    }

    [Fact]
    public async Task WaitAsync_RejectsADefaultTicket()
    {
        var signal = new NewEventsSignal();
        await Assert.ThrowsAsync<ArgumentException>(
            () => signal.WaitAsync(default, TimeSpan.FromSeconds(1)));
    }

    private static async Task WaitForWaitersAsync(NewEventsSignal signal, int count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (signal.WaitingCount < count)
            await Task.Delay(10, cts.Token);
    }
}
