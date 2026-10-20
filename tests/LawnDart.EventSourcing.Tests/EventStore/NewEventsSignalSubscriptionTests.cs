using System.Diagnostics;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Tests.EventStore;

public class NewEventsSignalSubscriptionTests
{
    [Fact]
    public async Task SameProcessSubscriber_WakesBeforeThePollInterval()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions
        {
            SubscriptionPollInterval = TimeSpan.FromSeconds(30)
        });

        await using var handle = store.Subscribe("sub-wake", fromSequence: 1);
        var read = ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(5));
        await WaitForWaitersAsync(store.NewEvents, 1);

        var started = Stopwatch.StartNew();
        await store.AppendAsync("s1", [Evt()]);
        var received = await read;

        Assert.True(started.Elapsed < TimeSpan.FromSeconds(2), $"woke after {started.Elapsed}");
        Assert.Equal(1, received[0].SequencePosition);
        Assert.Equal(1, store.NewEvents.Position);
        Assert.Equal(1, store.NewEvents.Generation);
    }

    [Fact]
    public async Task FailedAppend_DoesNotSignal()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("s1", [Evt()]);
        Assert.Equal(1, store.NewEvents.Generation);
        Assert.Equal(1, store.NewEvents.Position);

        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            store.AppendAsync("s1", [Evt()], expectedVersion: 0));

        Assert.Equal(1, store.NewEvents.Generation);
        Assert.Equal(1, store.NewEvents.Position);
        Assert.Single(await store.ReadStreamAsync("s1"));
    }

    [Fact]
    public async Task CommittedSave_SignalsOnce_WithTheNewestPosition()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("s1", [Evt(), Evt(), Evt()]);

        Assert.Equal(1, store.NewEvents.Generation);
        Assert.Equal(3, store.NewEvents.Position);

        await store.AppendAsync("s2", [Evt()]);
        Assert.Equal(2, store.NewEvents.Generation);
        Assert.Equal(4, store.NewEvents.Position);
    }

    [Fact]
    public async Task SignalNeverFired_PollStillDelivers()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions
        {
            SubscriptionPollInterval = TimeSpan.FromMilliseconds(50)
        });
        store.PublishNewEventsSignal = false;

        await using var handle = store.Subscribe("sub-poll", fromSequence: 1);
        var read = ReadExactlyAsync(handle, 2, TimeSpan.FromSeconds(3));
        await WaitForWaitersAsync(store.NewEvents, 1);

        await store.AppendAsync("s1", [Evt(), Evt()]);
        var received = await read;

        Assert.Equal(0, store.NewEvents.Generation);
        Assert.Equal(new long[] { 1, 2 }, received.Select(e => e.SequencePosition));
    }

    private static async Task WaitForWaitersAsync(NewEventsSignal signal, int count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (signal.WaitingCount < count)
            await Task.Delay(10, cts.Token);
    }

    private static async Task<List<SequencedEvent>> ReadExactlyAsync(
        ISubscriptionHandle handle,
        int count,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        var list = new List<SequencedEvent>(count);
        while (list.Count < count)
        {
            if (!await handle.Events.WaitToReadAsync(cts.Token))
                throw new TimeoutException($"Channel completed after {list.Count}/{count} events.");
            while (list.Count < count && handle.Events.TryRead(out var evt))
                list.Add(evt);
        }

        return list;
    }

    private static TestEvent Evt() => new(Guid.NewGuid(), DateTime.UtcNow);

    [EventTypeName("new-events-signal-subscription-tests.test-event")]
    private sealed record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}
