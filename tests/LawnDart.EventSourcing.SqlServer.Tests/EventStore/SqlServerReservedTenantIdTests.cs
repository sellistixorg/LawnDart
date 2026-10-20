using LawnDart;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.EventStore;

[Trait("Category", "Integration")]
public class SqlServerReservedTenantIdTests : IAsyncLifetime
{
    private MsSqlContainer? _sqlContainer;
    private string? _connectionString;

    public async Task InitializeAsync()
    {
        _sqlContainer = await MsSqlContainerStartup.StartAsync(() =>
            new MsSqlBuilder(MsSqlTestImage.Server2025)
                .WithPassword("Test123!")
                .Build());
        _connectionString = _sqlContainer.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        if (_sqlContainer is not null)
            await _sqlContainer.DisposeAsync();
    }

    [Fact]
    public async Task AppendAsync_ReservedTenantDefault_Throws()
    {
        var store = CreateStore(new SqlServerEventStoreOptions());
        var streamId = $"default:Order:{Guid.NewGuid()}";

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync(streamId, OneEvent(), metadata: new EventMetadata { TenantId = "default" }));
        Assert.Contains("reserved", ex.Message);
    }

    [Fact]
    public async Task AppendAsync_ValidateNamesFalse_AllowsReservedTenantDefault()
    {
        var store = CreateStore(new SqlServerEventStoreOptions { ValidateNames = false });
        await store.InitializeSchemaAsync();

        var streamId = $"default:Order:{Guid.NewGuid()}";
        await store.AppendAsync(streamId, OneEvent(), metadata: new EventMetadata { TenantId = "default" });

        var events = await store.ReadStreamAsync(streamId);
        Assert.Single(events);

        var stream = await store.GetStreamAsync(streamId);
        Assert.NotNull(stream);
        Assert.Equal("default", stream!.TenantId);
    }

    [Fact]
    public async Task AppendAsync_EmptyTenantId_Throws()
    {
        var store = CreateStore(new SqlServerEventStoreOptions { ValidateNames = false });
        var streamId = $":Order:{Guid.NewGuid()}";

        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync(streamId, OneEvent()));
    }

    [Fact]
    public async Task AppendAsync_MissingTenant_WhenRequired_Throws()
    {
        var store = CreateStore(new SqlServerEventStoreOptions());
        var streamId = $"Order:{Guid.NewGuid()}";

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync(streamId, OneEvent()));
        Assert.Contains("must include tenant ID", ex.Message);
    }

    private SqlServerEventStore CreateStore(SqlServerEventStoreOptions options)
    {
        var suffix = Guid.NewGuid().ToString("N");
        return new SqlServerEventStore(
            _connectionString!,
            tableName: $"Events_{suffix}",
            registryTableName: $"Streams_{suffix}",
            options: options);
    }

    private static IEvent[] OneEvent() => [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)];

    [EventTypeName("sql-server-reserved-tenant-id-tests.test-event")]
    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}
