using System.Diagnostics;
using Microsoft.Data.SqlClient;
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
    private MsSqlContainer? _container;
    private string _connectionString = string.Empty;
    private SqlServerEventStore _store = null!;

    public async Task InitializeAsync()
    {
        _container = await MsSqlContainerStartup.StartAsync(() =>
            new MsSqlBuilder(MsSqlTestImage.Server2025)
                .WithPassword("Test123!")
                .Build());
        _connectionString = _container.GetConnectionString();
        _store = CreateStore("dbo", "default", pollMs: 50, batchSize: 100, channelCapacity: 256);
        await _store.InitializeSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
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
            })
            .WithEventTypes(typeof(CatalogPlaceholderEvent));

        await using var sp = services.BuildServiceProvider();
        var store = (SqlServerEventStore)sp.GetRequiredKeyedService<IEventStore>("default");
        await store.InitializeSchemaAsync();

        var keyed = sp.GetRequiredKeyedService<IEventStoreSubscriptions>("default");
        var unkeyed = sp.GetRequiredService<IEventStoreSubscriptions>();
        Assert.Same(keyed, unkeyed);
        Assert.Same(store, keyed);
    }

    [Fact]
    public async Task HiddenCommit_DeliversWhenTheOpenFenceEnds_WithoutThePollInterval()
    {
        var schema = "h" + Guid.NewGuid().ToString("N")[..10];
        var store = new SqlServerEventStore(
            _connectionString,
            null,
            "Events",
            "Streams",
            true,
            new SqlServerEventStoreOptions
            {
                RequireTenantId = false,
                SchemaName = schema,
                ContextName = schema,
                SqlSubscriptionPollInterval = TimeSpan.FromSeconds(30),
                ReadHorizonGracePeriod = TimeSpan.FromMinutes(5)
            },
            null,
            null);
        await store.InitializeSchemaAsync();

        var log = (IEventLog)store;
        var stream = "s-" + Guid.NewGuid().ToString("N");
        await log.AppendAsync(stream, [new AppendEvent("hidden-probe", new byte[] { 1 })]);
        var head = await log.GetCurrentSequenceAsync();
        await using var handle = ((IEventLogSubscriptions)store).Subscribe("hidden", head + 1);
        await WaitForWaitersAsync(store.NewEvents, 1);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        await using var transactionDisposal = transaction.ConfigureAwait(false);
        await using (var insert = new SqlCommand(
            "INSERT INTO [" + schema + "].[AppendFence] DEFAULT VALUES", connection, transaction))
            await insert.ExecuteNonQueryAsync();

        var appended = await log.AppendAsync(stream, [new AppendEvent("hidden-probe", new byte[] { 2 })]);
        using (var duringHold = new CancellationTokenSource(TimeSpan.FromMilliseconds(400)))
        {
            var sawEarly = false;
            try
            {
                sawEarly = await handle.Events.WaitToReadAsync(duringHold.Token);
            }
            catch (OperationCanceledException)
            {
                sawEarly = false;
            }

            Assert.False(sawEarly);
        }

        var began = Stopwatch.GetTimestamp();
        await transaction.RollbackAsync();
        using var wait = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var received = await handle.Events.ReadAsync(wait.Token);
        var elapsed = Stopwatch.GetElapsedTime(began);
        Assert.True(elapsed < TimeSpan.FromSeconds(2), "delivered after " + elapsed);
        Assert.Equal(appended.SequencePositions[0], received.SequencePosition);
    }





    private static async Task WaitForWaitersAsync(NewEventsSignal signal, int count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (signal.WaitingCount < count)
            await Task.Delay(10, cts.Token);
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
