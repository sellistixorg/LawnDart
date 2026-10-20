using System.Diagnostics;
using LawnDart.EventStore;

namespace LawnDart.Backends.Contract.Tests;

public abstract class SubscriptionContractTests : ContractSuite
{
    protected SubscriptionContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task Global_CatchUp_Then_Live()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        await using var handle = opened.Subscriptions.Subscribe("sub-global", fromSequence: 1);
        var readTask = ReadExactlyAsync(handle, 3, TimeSpan.FromSeconds(15));
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        var received = await readTask;
        Assert.Equal(new long[] { 1, 2, 3 }, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task Stream_Scope_Isolates_Other_Streams()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await opened.Store.AppendAsync("a", [ContractEvent.Tick()]);
        await opened.Store.AppendAsync("b", [ContractEvent.Tick()]);
        await opened.Store.AppendAsync("a", [ContractEvent.Tick()]);
        await using var handle = opened.Subscriptions.Subscribe(
            "sub-stream",
            fromSequence: 1,
            EventSubscriptionFilter.ForStream("a"));
        var received = await ReadExactlyAsync(handle, 2, TimeSpan.FromSeconds(15));
        Assert.All(received, e => Assert.Equal("a", e.StreamId));
        Assert.Equal(new long[] { 1, 3 }, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task Query_Scope_Matches_ReadByQuery()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()], tags: ["order:1"]);
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()], tags: ["other"]);
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()], tags: ["order:1"]);
        var query = Query.FromItems(QueryItem.ByTags("order:1"));
        var expected = (await opened.Store.ReadByQueryAsync(query)).Events.Select(e => e.SequencePosition).ToArray();
        await using var handle = opened.Subscriptions.Subscribe(
            "sub-query",
            fromSequence: 1,
            EventSubscriptionFilter.ForQuery(query));
        var received = await ReadExactlyAsync(handle, expected.Length, TimeSpan.FromSeconds(15));
        Assert.Equal(expected, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task FromSequence_Is_Inclusive()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick(), ContractEvent.Tick(), ContractEvent.Tick()]);
        await using var handle = opened.Subscriptions.Subscribe("sub-inclusive", fromSequence: 2);
        var received = await ReadExactlyAsync(handle, 2, TimeSpan.FromSeconds(15));
        Assert.Equal(new long[] { 2, 3 }, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task Idle_Then_Append_Delivers()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await using var handle = opened.Subscriptions.Subscribe("sub-idle", fromSequence: 1);
        await Task.Delay(80);
        var readTask = ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(10));
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        var received = await readTask;
        Assert.Equal(1, received[0].SequencePosition);
    }

    [Fact]
    public async Task Client_Reconnect_With_LastSequence_Plus_One()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick(), ContractEvent.Tick(), ContractEvent.Tick()]);
        await using (var first = opened.Subscriptions.Subscribe("sub-reconnect", fromSequence: 1))
        {
            var batch = await ReadExactlyAsync(first, 2, TimeSpan.FromSeconds(15));
            Assert.Equal(new long[] { 1, 2 }, batch.Select(e => e.SequencePosition));
        }

        await using var second = opened.Subscriptions.Subscribe("sub-reconnect-2", fromSequence: 3);
        var rest = await ReadExactlyAsync(second, 1, TimeSpan.FromSeconds(15));
        Assert.Equal(3, rest[0].SequencePosition);
    }

    [Fact]
    public async Task Dispose_Stops_Delivery()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        var handle = opened.Subscriptions.Subscribe("sub-dispose", fromSequence: 1);
        _ = await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(15));
        await handle.DisposeAsync();
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        await Task.Delay(150);
        Assert.True(handle.Events.Completion.IsCompleted);
        Assert.False(await handle.Events.WaitToReadAsync());
    }

    [Fact]
    public async Task Slow_Consumer_No_Silent_Loss()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(subscriptionChannelCapacity: 1));
        await using var handle = opened.Subscriptions.Subscribe("sub-slow", fromSequence: 1);
        for (var i = 0; i < 5; i++)
            await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        var received = await ReadExactlyAsync(handle, 5, TimeSpan.FromSeconds(30));
        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task MultiContext_Subscriptions_Are_Isolated()
    {
        var ordering = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var catalog = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await using var oh = ordering.Subscriptions.Subscribe("o", fromSequence: 1);
        await using var ch = catalog.Subscriptions.Subscribe("c", fromSequence: 1);
        await ordering.Store.AppendAsync("Order:1", [ContractEvent.Tick()]);
        await catalog.Store.AppendAsync("Product:1", [ContractEvent.Tick()]);
        var o = await ReadExactlyAsync(oh, 1, TimeSpan.FromSeconds(15));
        var c = await ReadExactlyAsync(ch, 1, TimeSpan.FromSeconds(15));
        Assert.Equal("Order:1", o[0].StreamId);
        Assert.Equal("Product:1", c[0].StreamId);
    }

    [Fact]
    public async Task Load_Smoke_Sustained_Append_With_Global_Subscriber()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await using var handle = opened.Subscriptions.Subscribe("sub-load", fromSequence: 1);
        var readTask = ReadExactlyAsync(handle, 40, TimeSpan.FromSeconds(60));
        for (var i = 0; i < 40; i++)
            await opened.Store.AppendAsync("load", [ContractEvent.Tick()]);
        var received = await readTask;
        Assert.Equal(40, received.Count);
        Assert.Equal(Enumerable.Range(1, 40).Select(i => (long)i), received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task SameProcessSubscriber_WakesBeforeThePollInterval()
    {
        var opened = await Backend.OpenEventStoreAsync(
            ContractStores.Isolated(subscriptionPollInterval: TimeSpan.FromSeconds(30)));
        await using var handle = opened.Subscriptions.Subscribe("sub-wake", fromSequence: 1);
        var read = ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(5));
        await WaitForWaitersAsync(opened.Signals.NewEvents, 1);
        var started = Stopwatch.StartNew();
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        var received = await read;
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(2), $"woke after {started.Elapsed}");
        Assert.Equal(1, received[0].SequencePosition);
        Assert.True(opened.Signals.NewEvents.Position >= 1);
        Assert.Equal(1, opened.Signals.NewEvents.Generation);
    }

    [Fact]
    public async Task FailedAppend_DoesNotSignal()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        var generation = opened.Signals.NewEvents.Generation;
        var position = opened.Signals.NewEvents.Position;
        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            opened.Store.AppendAsync("s1", [ContractEvent.Tick()], expectedVersion: 0));
        Assert.Equal(generation, opened.Signals.NewEvents.Generation);
        Assert.Equal(position, opened.Signals.NewEvents.Position);
        Assert.Single(await opened.Store.ReadStreamAsync("s1"));
    }

    [ContractFact]
    public async Task Signal_IsRaisedOnlyAfterCommit()
    {
        SkipUnless(
            Cap.CanPauseBeforeCommit,
            "In-memory append commits before AppendAsync returns, so there is no moment to pause before commit.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var append = opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.Equal(0, opened.Signals.NewEvents.Generation);
        Assert.False(append.IsCompleted);
        release.TrySetResult();
        var result = await append.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.Equal(1, opened.Signals.NewEvents.Generation);
        Assert.Equal(result.SequencePositions[^1], opened.Signals.NewEvents.Position);
    }

    [Fact]
    public async Task SignalNeverFired_PollStillDelivers()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(publishNewEventsSignal: false));
        await using var handle = opened.Subscriptions.Subscribe("sub-poll", fromSequence: 1);
        var read = ReadExactlyAsync(handle, 2, TimeSpan.FromSeconds(5));
        await WaitForWaitersAsync(opened.Signals.NewEvents, 1);
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick(), ContractEvent.Tick()]);
        var received = await read;
        Assert.Equal(0, opened.Signals.NewEvents.Generation);
        Assert.Equal(new long[] { 1, 2 }, received.Select(e => e.SequencePosition));
    }

    [ContractFact]
    public async Task PagedSubscription_DeliversEveryEvent()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(subscriptionBatchSize: 2));
        for (var i = 0; i < 5; i++)
            await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        await using var handle = opened.Subscriptions.Subscribe("paged", fromSequence: 1);
        var received = await ReadExactlyAsync(handle, 5, TimeSpan.FromSeconds(15));
        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, received.Select(e => e.SequencePosition));
    }

    [ContractFact]
    public async Task FailedSubscription_CompletesWithSubscriptionFailedException()
    {
        SkipUnless(
            Cap.CanBreakLiveReads,
            "In-memory has no table to rename while a subscription is reading.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        await using var handle = opened.Subscriptions.Subscribe("failsub", fromSequence: 1);
        await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(15));
        await opened.Hooks.BreakLiveReadsAsync();
        var completed = await Task.WhenAny(handle.Events.Completion, Task.Delay(TimeSpan.FromSeconds(20)));
        Assert.Same(handle.Events.Completion, completed);
        var ex = await Assert.ThrowsAsync<EventSubscriptionFailedException>(() => handle.Events.Completion);
        Assert.Equal("failsub", ex.SubscriberId);
    }
}
