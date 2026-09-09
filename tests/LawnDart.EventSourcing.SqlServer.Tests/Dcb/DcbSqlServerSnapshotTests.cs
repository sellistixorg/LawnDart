using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventSourcing.SqlServer.Snapshots;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Snapshots;
using LawnDart.TestUtilities;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.Dcb;

/// <summary>
/// SQL DCB snapshots: hashed PK, plaintext tags column, save/load, corrupt fallback,
/// <c>HandleCommandAsync</c> writes, keyed <c>WithSnapshots</c>.
/// </summary>
[Trait("Category", "Integration")]
public sealed class DcbSqlServerSnapshotTests : IAsyncLifetime
{
    private MsSqlContainer? _container;
    private string _connectionString = string.Empty;
    private SqlServerEventStore? _eventStore;
    private SqlServerSnapshotStore? _snapshotStore;
    private SqlServerEventStoreOptions _options = null!;

    public async Task InitializeAsync()
    {
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("Test123!")
            .Build();
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();

        _options = new SqlServerEventStoreOptions
        {
            ConnectionString = _connectionString,
            RequireTenantId = false,
            SchemaName = "dcb_snap"
        };

        _eventStore = new SqlServerEventStore(
            _connectionString,
            serializer: null,
            tableName: "Events",
            registryTableName: "Streams",
            enableRegistry: true,
            _options,
            outboxWriter: null,
            logger: null);
        await _eventStore.InitializeSchemaAsync();

        _snapshotStore = new SqlServerSnapshotStore(_options, "Events");
        await _snapshotStore.InitializeSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container != null)
            await _container.DisposeAsync();
    }

    private DcbRepository CreateRepository(IDcbSnapshotStore? snapshots, ISnapshotStrategyResolver? resolver = null)
        => new(
            _eventStore!,
            new TestMetadataProvider(),
            new TestTenantContextProvider(null),
            Options.Create(new LawnDartOptions()),
            tagProvider: null,
            authorizationService: null,
            logger: null,
            eventSourcingOptions: Options.Create(new EventSourcing.EventSourcingOptions { EnforceDcbTenantIsolation = false }),
            dcbSnapshotStore: snapshots,
            strategyResolver: resolver);

    [Fact]
    public async Task SaveThenLoad_RoundTripsStateAndPlaintextTags()
    {
        var tags = new[] { "product:sku-1", "warehouse:east" };
        var dcbId = DcbSnapshotId.FromLoadTags(tags);
        var state = new InventoryState { Units = 42 };

        await _snapshotStore!.SaveDcbSnapshotAsync(dcbId, 17, state, loadTags: tags);

        var (restored, info) = await _snapshotStore.LoadDcbSnapshotAsync<InventoryState>(dcbId);
        Assert.NotNull(info);
        Assert.Equal(17, info!.GlobalSequence);
        Assert.Equal(42, restored!.Units);

        var storedTags = await _snapshotStore.LoadDcbSnapshotTagsAsync(dcbId);
        Assert.Equal(tags, storedTags);
        Assert.Equal(DcbSnapshotId.HexLength, dcbId.Length);
    }

    [Fact]
    public async Task CorruptSnapshot_FallsBackToFullReplay()
    {
        var tags = new[] { "product:sku-corrupt" };
        var dcbId = DcbSnapshotId.FromLoadTags(tags);
        var repo = CreateRepository(_snapshotStore);

        await repo.AppendWithContextAsync(
            [new InventoryCounted(Guid.NewGuid(), DateTime.UtcNow, 5),
             new InventoryCounted(Guid.NewGuid(), DateTime.UtcNow, 7)],
            tags);

        var good = await repo.GetStateAsync<InventoryState>(tags);
        Assert.Equal(12, good.Units);

        await _snapshotStore!.SaveDcbSnapshotAsync(dcbId, 2, good, loadTags: tags);

        await using (var conn = new SqlConnection(_connectionString))
        {
            await conn.OpenAsync();
            await using var cmd = new SqlCommand(
                $"UPDATE {_snapshotStore.QualifiedDcbTable} SET [StateData] = N'not-json' WHERE [DcbId] = @Id",
                conn);
            var p = cmd.Parameters.Add("@Id", System.Data.SqlDbType.Char, 64);
            p.Value = dcbId;
            var rows = await cmd.ExecuteNonQueryAsync();
            Assert.Equal(1, rows);
        }

        var (corrupt, info) = await _snapshotStore.LoadDcbSnapshotAsync<InventoryState>(dcbId);
        Assert.Null(info);
        Assert.Null(corrupt);

        var rebuilt = await repo.GetStateAsync<InventoryState>(tags);
        Assert.Equal(12, rebuilt.Units);
    }

    [Fact]
    public async Task HandleCommandAsync_EventCountStrategy1_WritesHashedSnapshot_ThenReloadUsesIt()
    {
        var resolver = new SnapshotStrategyResolver();
        resolver.RegisterForDcb<InventoryEntity>(new EventCountSnapshotStrategy(1));
        var repo = CreateRepository(_snapshotStore, resolver);
        var tags = new[] { "product:sku-cmd" };
        var dcbId = DcbSnapshotId.FromLoadTags(tags);

        var entity = await repo.CreateEntityAsync<InventoryEntity>(tags);
        await repo.HandleCommandAsync(entity, new CountInventory(3), new CommandMetadata { UserId = "u" });

        await WaitForSnapshotAsync(dcbId);

        var storedTags = await _snapshotStore!.LoadDcbSnapshotTagsAsync(dcbId);
        Assert.Equal(tags, storedTags);

        var (snapped, info) = await _snapshotStore.LoadDcbSnapshotAsync<InventoryEntity>(dcbId);
        Assert.NotNull(info);
        Assert.NotNull(snapped);
        Assert.Equal(3, snapped!.State.Units);

        var loaded = await repo.GetOrCreateEntityAsync<InventoryEntity>(tags);
        Assert.Equal(3, loaded.State.Units);
        Assert.Equal(0, loaded.EventsSinceLastSnapshot);
    }

    [Fact]
    public async Task GetStateAsync_SameHashedKey_RestoresSnapshotAndReplaysDelta()
    {
        var tags = new[] { "product:sku-delta" };
        var dcbId = DcbSnapshotId.FromLoadTags(tags);
        var repo = CreateRepository(_snapshotStore);

        await repo.AppendWithContextAsync(
            Enumerable.Range(0, 10).Select(i => (IEvent)new InventoryCounted(Guid.NewGuid(), DateTime.UtcNow, 1)).ToArray(),
            tags);

        var atTen = await repo.GetStateAsync<InventoryState>(tags);
        Assert.Equal(10, atTen.Units);
        var max = await _eventStore!.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags(tags)));
        await _snapshotStore!.SaveDcbSnapshotAsync(dcbId, max, atTen, loadTags: tags);

        await repo.AppendWithContextAsync(
            Enumerable.Range(0, 5).Select(i => (IEvent)new InventoryCounted(Guid.NewGuid(), DateTime.UtcNow, 1)).ToArray(),
            tags);

        var restored = await repo.GetStateAsync<InventoryState>(tags);
        Assert.Equal(15, restored.Units);
    }

    [Fact]
    public void UseSqlServer_WithSnapshots_RegistersKeyedStoreOnDcbRepository()
    {
        var services = new ServiceCollection();
        services.AddLawnDart();
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));

        services.AddBoundedContext("ordering")
            .UseSqlServer(opt =>
            {
                opt.ConnectionString = _connectionString;
                opt.SchemaName = "ordering_di";
                opt.RequireTenantId = false;
            })
            .WithSnapshots(cfg => cfg.RegisterForDcb<InventoryEntity>(new EventCountSnapshotStrategy(100)));

        using var sp = services.BuildServiceProvider();
        var keyed = sp.GetRequiredKeyedService<IDcbSnapshotStore>("ordering");
        Assert.IsType<SqlServerSnapshotStore>(keyed);
        var repo = sp.GetRequiredKeyedService<IDcbRepository>("ordering");
        Assert.NotNull(repo);
        Assert.Null(sp.GetService<IDcbSnapshotStore>());
    }

    private async Task WaitForSnapshotAsync(string dcbId)
    {
        for (var i = 0; i < 50; i++)
        {
            var (_, info) = await _snapshotStore!.LoadDcbSnapshotAsync<InventoryEntity>(dcbId);
            if (info != null)
                return;
            await Task.Delay(50);
        }

        throw new TimeoutException($"DCB snapshot for {dcbId} was not written.");
    }

    public sealed record InventoryCounted(Guid Id, DateTime Timestamp, int Units) : IEvent;

    public sealed class InventoryState : IState
    {
        public int Units { get; set; }

        public void Apply(InventoryCounted e) => Units += e.Units;
    }

    public sealed class CountInventory : ICommand
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public int Units { get; }

        public CountInventory(int units) => Units = units;
    }

    public sealed class InventoryEntity : DcbEntity<InventoryState>
    {
        public void Handle(CountInventory count) =>
            Emit(new InventoryCounted(Guid.NewGuid(), DateTime.UtcNow, count.Units), Tags.ToArray());

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is InventoryCounted counted)
                State.Apply(counted);
        }
    }
}
