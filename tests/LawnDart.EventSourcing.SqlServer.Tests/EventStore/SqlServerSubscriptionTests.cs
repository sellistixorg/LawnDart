using Microsoft.Extensions.DependencyInjection;
using LawnDart;
using LawnDart.EventSourcing.Serialization;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Serialization;
using LawnDart.TestUtilities;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.EventStore;

/// <summary>
/// Portable subscription integration tests against real SQL Server (Testcontainers).
/// </summary>
[Trait("Category", "Integration")]
public class SqlServerSubscriptionTests : IAsyncLifetime
{
    private readonly MsSqlContainer _container;
    private string _connectionString = string.Empty;
    private SqlServerEventStore _store = null!;

    public SqlServerSubscriptionTests()
    {
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("Test123!")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();
        _store = CreateStore("dbo", "default", pollMs: 50, batchSize: 100, channelCapacity: 256);
        await _store.InitializeSchemaAsync();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();

    [Fact]
    public async Task Global_CatchUp_Then_Live()
    {
        await _store.AppendAsync("s1", [Evt()]);
        await _store.AppendAsync("s1", [Evt()]);

        await using var handle = _store.Subscribe("sub-global", fromSequence: 1);
        var readTask = ReadExactlyAsync(handle, 3, TimeSpan.FromSeconds(15));
        await _store.AppendAsync("s1", [Evt()]);

        var received = await readTask;
        Assert.Equal(new long[] { 1, 2, 3 }, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task Stream_Scope_Isolates_Other_Streams()
    {
        await _store.AppendAsync("a", [Evt()]);
        await _store.AppendAsync("b", [Evt()]);
        await _store.AppendAsync("a", [Evt()]);

        await using var handle = _store.Subscribe(
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
        await _store.AppendAsync("s1", [Evt()], tags: ["order:1"]);
        await _store.AppendAsync("s1", [Evt()], tags: ["other"]);
        await _store.AppendAsync("s1", [Evt()], tags: ["order:1"]);

        var query = Query.FromItems(QueryItem.ByTags("order:1"));
        var expected = (await _store.ReadByQueryAsync(query)).Events
            .Select(e => e.SequencePosition)
            .ToArray();

        await using var handle = _store.Subscribe(
            "sub-query",
            fromSequence: 1,
            EventSubscriptionFilter.ForQuery(query));

        var received = await ReadExactlyAsync(handle, expected.Length, TimeSpan.FromSeconds(15));
        Assert.Equal(expected, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task FromSequence_Is_Inclusive()
    {
        await _store.AppendAsync("s1", [Evt(), Evt(), Evt()]);

        await using var handle = _store.Subscribe("sub-inclusive", fromSequence: 2);
        var received = await ReadExactlyAsync(handle, 2, TimeSpan.FromSeconds(15));
        Assert.Equal(new long[] { 2, 3 }, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task Idle_Then_Append_Delivers()
    {
        await using var handle = _store.Subscribe("sub-idle", fromSequence: 1);
        // Allow one idle poll cycle with empty store.
        await Task.Delay(80);

        var readTask = ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(10));
        await _store.AppendAsync("s1", [Evt()]);
        var received = await readTask;

        Assert.Equal(1, received[0].SequencePosition);
    }

    [Fact]
    public async Task Client_Reconnect_With_LastSequence_Plus_One()
    {
        await _store.AppendAsync("s1", [Evt(), Evt(), Evt()]);

        await using (var first = _store.Subscribe("sub-reconnect", fromSequence: 1))
        {
            var batch = await ReadExactlyAsync(first, 2, TimeSpan.FromSeconds(15));
            Assert.Equal(new long[] { 1, 2 }, batch.Select(e => e.SequencePosition));
            // Client-owned cursor: lastApplied = 2 → reconnect with 3.
        }

        await using var second = _store.Subscribe("sub-reconnect", fromSequence: 3);
        var rest = await ReadExactlyAsync(second, 1, TimeSpan.FromSeconds(15));
        Assert.Equal(3, rest[0].SequencePosition);
    }

    [Fact]
    public async Task Dispose_Stops_Delivery()
    {
        await _store.AppendAsync("s1", [Evt()]);
        var handle = _store.Subscribe("sub-dispose", fromSequence: 1);
        _ = await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(15));
        await handle.DisposeAsync();

        await _store.AppendAsync("s1", [Evt()]);
        await Task.Delay(150);
        Assert.True(handle.Events.Completion.IsCompleted);
        Assert.False(await handle.Events.WaitToReadAsync());
    }

    [Fact]
    public async Task Slow_Consumer_No_Silent_Loss()
    {
        var store = CreateStore("slow", "slow", pollMs: 40, batchSize: 50, channelCapacity: 1);
        await store.InitializeSchemaAsync();

        await using var handle = store.Subscribe("sub-slow", fromSequence: 1);
        for (var i = 0; i < 5; i++)
            await store.AppendAsync("s1", [Evt()]);

        var received = await ReadExactlyAsync(handle, 5, TimeSpan.FromSeconds(30));
        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task MultiContext_Sql_Subscriptions_Are_Isolated()
    {
        var ordering = CreateStore("ordering", "ordering", pollMs: 50);
        var catalog = CreateStore("catalog", "catalog", pollMs: 50);
        await ordering.InitializeSchemaAsync();
        await catalog.InitializeSchemaAsync();

        await using var oh = ordering.Subscribe("o", fromSequence: 1);
        await using var ch = catalog.Subscribe("c", fromSequence: 1);

        await ordering.AppendAsync("Order:1", [Evt()]);
        await catalog.AppendAsync("Product:1", [Evt()]);

        var o = await ReadExactlyAsync(oh, 1, TimeSpan.FromSeconds(15));
        var c = await ReadExactlyAsync(ch, 1, TimeSpan.FromSeconds(15));
        Assert.Equal("Order:1", o[0].StreamId);
        Assert.Equal("Product:1", c[0].StreamId);
    }

    [Fact]
    public async Task Load_Smoke_Sustained_Append_With_Global_Subscriber()
    {
        await using var handle = _store.Subscribe("sub-load", fromSequence: 1);
        var readTask = ReadExactlyAsync(handle, 40, TimeSpan.FromSeconds(60));

        for (var i = 0; i < 40; i++)
            await _store.AppendAsync("load", [Evt()]);

        var received = await readTask;
        Assert.Equal(40, received.Count);
        Assert.Equal(Enumerable.Range(1, 40).Select(i => (long)i), received.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task UseSqlServer_Registers_Keyed_Subscriptions()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.AddSingleton<IEventSerializer, JsonEventSerializer>();
        services.Configure<LawnDartOptions>(_ => { });

        services.AddBoundedContext("default")
            .UseSqlServer(o =>
            {
                o.ConnectionString = _connectionString;
                o.RequireTenantId = false;
                o.SchemaName = "di_default";
                o.SqlSubscriptionPollInterval = TimeSpan.FromMilliseconds(50);
            });

        await using var sp = services.BuildServiceProvider();
        var store = (SqlServerEventStore)sp.GetRequiredKeyedService<IEventStore>("default");
        await store.InitializeSchemaAsync();

        var keyed = sp.GetRequiredKeyedService<IEventStoreSubscriptions>("default");
        var unkeyed = sp.GetRequiredService<IEventStoreSubscriptions>();
        Assert.Same(keyed, unkeyed);
        Assert.Same(store, keyed);
    }

    private SqlServerEventStore CreateStore(
        string schema,
        string contextName,
        int pollMs = 75,
        int batchSize = 500,
        int channelCapacity = 10_000) =>
        new(
            _connectionString,
            null,
            "Events",
            "Streams",
            true,
            new SqlServerEventStoreOptions
            {
                RequireTenantId = false,
                SchemaName = schema,
                ContextName = contextName,
                SqlSubscriptionPollInterval = TimeSpan.FromMilliseconds(pollMs),
                SqlSubscriptionBatchSize = batchSize,
                SubscriptionChannelCapacity = channelCapacity
            },
            null,
            null);

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

    private static StubEvent Evt() => new(Guid.NewGuid(), DateTime.UtcNow);
[EventTypeName("sql-server-subscription-tests.stub-event")]

    private sealed record StubEvent(Guid Id, DateTime Timestamp) : IEvent;
}
