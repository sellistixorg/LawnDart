using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.Snapshots;
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
        _container = await MsSqlContainerStartup.StartAsync(() =>
            new MsSqlBuilder(MsSqlTestImage.Server2025)
                .WithPassword("Test123!")
                .Build());
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

    private DcbRepository CreateRepository(
        IDcbSnapshotStore? snapshots,
        ISnapshotStrategyResolver? resolver = null,
        ISnapshotWriteQueue? writeQueue = null)
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
            strategyResolver: resolver,
            snapshotWriteQueue: writeQueue);





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
            .WithEventTypes(typeof(CatalogPlaceholderEvent))
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
[EventTypeName("dcb-sql-server-snapshot-tests.inventory-counted")]

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
