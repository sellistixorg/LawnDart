using Microsoft.Extensions.DependencyInjection;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.TestUtilities;
using Xunit;

namespace LawnDart.EventSourcing.Tests.EventStore;

public class InMemorySubscriptionTests
{
    [Fact]
    public async Task Global_CatchUp_Then_Live_Is_Continuous()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("s1", [Evt()]);
        await store.AppendAsync("s1", [Evt()]);

        await using var handle = store.Subscribe("sub-global", fromSequence: 1);
        var received = new List<SequencedEvent>();

        var readTask = Task.Run(async () =>
        {
            await foreach (var e in handle.Events.ReadAllAsync())
            {
                received.Add(e);
                if (received.Count >= 3)
                    break;
            }
        });

        await store.AppendAsync("s1", [Evt()]);
        await readTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(new long[] { 1, 2, 3 }, received.Select(e => e.SequencePosition));
        Assert.Equal(3, handle.LastDeliveredSequence);
    }

    [Fact]
    public async Task Stream_Scope_Isolates_Other_Streams()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("a", [Evt()]);
        await store.AppendAsync("b", [Evt()]);
        await store.AppendAsync("a", [Evt()]);

        await using var handle = store.Subscribe(
            "sub-stream",
            fromSequence: 1,
            EventSubscriptionFilter.ForStream("a"));

        var received = await ReadExactlyAsync(handle, count: 2, TimeSpan.FromSeconds(5));
        Assert.All(received, e => Assert.Equal("a", e.StreamId));
        Assert.Equal(new long[] { 1, 3 }, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task Query_Scope_Matches_ReadByQuery()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("s1", [Evt()], tags: ["order:1"]);
        await store.AppendAsync("s1", [Evt()], tags: ["other"]);
        await store.AppendAsync("s1", [Evt()], tags: ["order:1"]);

        var query = Query.FromItems(QueryItem.ByTags("order:1"));
        var expected = (await store.ReadByQueryAsync(query)).Events
            .Select(e => e.SequencePosition)
            .ToArray();

        await using var handle = store.Subscribe(
            "sub-query",
            fromSequence: 1,
            EventSubscriptionFilter.ForQuery(query));

        var received = await ReadExactlyAsync(handle, expected.Length, TimeSpan.FromSeconds(5));
        Assert.Equal(expected, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task FromSequence_Is_Inclusive()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("s1", [Evt(), Evt(), Evt()]);

        await using var handle = store.Subscribe("sub-inclusive", fromSequence: 2);
        var received = await ReadExactlyAsync(handle, count: 2, TimeSpan.FromSeconds(5));
        Assert.Equal(new long[] { 2, 3 }, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task Dispose_Stops_Delivery_And_Unregisters()
    {
        var store = new InMemoryEventStore(
            options: new InMemoryEventStoreOptions { SubscriptionChannelCapacity = 8 });
        await store.AppendAsync("s1", [Evt()]);

        var handle = store.Subscribe("sub-dispose", fromSequence: 1);
        _ = await ReadExactlyAsync(handle, count: 1, TimeSpan.FromSeconds(5));
        await handle.DisposeAsync();

        await store.AppendAsync("s1", [Evt()]);
        await Task.Delay(100);

        Assert.True(handle.Events.Completion.IsCompleted);
        Assert.False(await handle.Events.WaitToReadAsync());
    }

    [Fact]
    public async Task Slow_Consumer_Blocks_Without_Silent_Loss()
    {
        var store = new InMemoryEventStore(
            options: new InMemoryEventStoreOptions { SubscriptionChannelCapacity = 1 });

        await using var handle = store.Subscribe("sub-slow", fromSequence: 1);

        // Fill catch-up / live path with more events than capacity.
        for (var i = 0; i < 5; i++)
            await store.AppendAsync("s1", [Evt()]);

        // Give the delivery loop time to block on the full channel.
        await Task.Delay(100);

        var received = await ReadExactlyAsync(handle, count: 5, TimeSpan.FromSeconds(10));
        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task Multiple_Independent_Subscribers()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("s1", [Evt()]);

        await using var a = store.Subscribe("a", fromSequence: 1);
        await using var b = store.Subscribe("b", fromSequence: 1);

        var ra = ReadExactlyAsync(a, 2, TimeSpan.FromSeconds(5));
        var rb = ReadExactlyAsync(b, 2, TimeSpan.FromSeconds(5));
        await store.AppendAsync("s1", [Evt()]);

        Assert.Equal(new long[] { 1, 2 }, (await ra).Select(e => e.SequencePosition));
        Assert.Equal(new long[] { 1, 2 }, (await rb).Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task Concurrent_Append_During_CatchUp_Is_Gap_Free()
    {
        var store = new InMemoryEventStore();
        for (var i = 0; i < 50; i++)
            await store.AppendAsync("s1", [Evt()]);

        await using var handle = store.Subscribe("sub-race", fromSequence: 1);
        var appendTask = Task.Run(async () =>
        {
            for (var i = 0; i < 50; i++)
                await store.AppendAsync("s1", [Evt()]);
        });

        var received = await ReadExactlyAsync(handle, count: 100, TimeSpan.FromSeconds(15));
        await appendTask;

        Assert.Equal(100, received.Count);
        Assert.Equal(Enumerable.Range(1, 100).Select(i => (long)i), received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task Keyed_MultiContext_Subscriptions_Are_Isolated()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });

        services.AddBoundedContext("ordering").UseInMemory();
        services.AddBoundedContext("catalog").UseInMemory();

        await using var sp = services.BuildServiceProvider();
        var orderingSubs = sp.GetRequiredKeyedService<IEventStoreSubscriptions>("ordering");
        var catalogSubs = sp.GetRequiredKeyedService<IEventStoreSubscriptions>("catalog");
        var orderingStore = sp.GetRequiredKeyedService<IEventStore>("ordering");
        var catalogStore = sp.GetRequiredKeyedService<IEventStore>("catalog");

        await using var orderingHandle = orderingSubs.Subscribe("o", fromSequence: 1);
        await using var catalogHandle = catalogSubs.Subscribe("c", fromSequence: 1);

        await orderingStore.AppendAsync("Order:1", [Evt()]);
        await catalogStore.AppendAsync("Product:1", [Evt()]);

        var o = await ReadExactlyAsync(orderingHandle, 1, TimeSpan.FromSeconds(5));
        var c = await ReadExactlyAsync(catalogHandle, 1, TimeSpan.FromSeconds(5));

        Assert.Equal("Order:1", o[0].StreamId);
        Assert.Equal("Product:1", c[0].StreamId);
        Assert.Equal(1, o[0].SequencePosition);
        Assert.Equal(1, c[0].SequencePosition);
    }

    [Fact]
    public async Task Default_Context_Registers_Unkeyed_Subscriptions()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });
        services.AddBoundedContext("default").UseInMemory();

        await using var sp = services.BuildServiceProvider();
        var keyed = sp.GetRequiredKeyedService<IEventStoreSubscriptions>("default");
        var unkeyed = sp.GetRequiredService<IEventStoreSubscriptions>();
        Assert.Same(keyed, unkeyed);
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

    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}
