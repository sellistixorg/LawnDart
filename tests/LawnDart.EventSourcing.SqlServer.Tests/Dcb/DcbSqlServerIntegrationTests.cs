using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.TestUtilities;
using Testcontainers.MsSql;
using Xunit;

namespace LawnDart.EventSourcing.SqlServer.Tests.Dcb;

/// <summary>
/// Integration tests for DCB operations with SQL Server backend.
/// </summary>
[Trait("Category", "Integration")]
public class DcbSqlServerIntegrationTests : IAsyncLifetime
{
    private MsSqlContainer? _container;
    private string? _connectionString;
    private SqlServerEventStore? _eventStore;

    public async Task InitializeAsync()
    {
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .Build();

        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();

        _eventStore = new SqlServerEventStore(
            _connectionString,
            null,
            "Events",
            "Streams",
            true,
            new SqlServerEventStoreOptions { RequireTenantId = false },
            null,
            null);

        await _eventStore.InitializeSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
        }
    }

    private static IOptions<LawnDartOptions> DefaultLawnDartOptions() =>
        Options.Create(new LawnDartOptions());

    [Fact]
    public async Task GetStateAsync_WithSqlBackend_RebuildsStateFromEvents()
    {
        // Arrange
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("test-tenant");

        var repository = new DcbRepository(
            _eventStore!,
            metadataProvider,
            tenantProvider,
            DefaultLawnDartOptions(),
            null,
            null,
            null);

        var tags = new[] { "order:order-1", "customer:customer-1" };
        var events = new IEvent[]
        {
            new TestDcbEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = 100 },
            new TestDcbEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = 50 }
        };

        await repository.AppendWithContextAsync(events, tags);

        // Act
        var state = await repository.GetStateAsync<TestDcbState>(tags);

        // Assert
        Assert.NotNull(state);
        Assert.Equal(150, state.TotalValue);
        Assert.Equal(2, state.EventCount);
    }

    [Fact]
    public async Task AppendWithContextAsync_WithAppendCondition_EnforcesConcurrency()
    {
        // Arrange
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("test-tenant");

        var repository = new DcbRepository(
            _eventStore!,
            metadataProvider,
            tenantProvider,
            DefaultLawnDartOptions(),
            null,
            null,
            null);

        var tags = new[] { "order:order-2", "customer:customer-2" };
        var event1 = new TestDcbEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = 100 };

        // First append
        var positions = await repository.AppendWithContextAsync(new[] { event1 }, tags);
        var lastPosition = positions[0];

        // Act & Assert - Append with condition that should succeed
        var event2 = new TestDcbEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = 50 };
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags(tags)),
            lastPosition);

        var newPositions = await repository.AppendWithContextAsync(new[] { event2 }, tags, condition);
        Assert.NotEmpty(newPositions);

        // Try to append again with same condition - should fail
        var event3 = new TestDcbEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = 25 };
        await Assert.ThrowsAsync<ConcurrencyException>(async () =>
            await repository.AppendWithContextAsync(new[] { event3 }, tags, condition));
    }

    [Fact]
    public async Task DcbOperations_WithMultipleTags_QueriesCorrectly()
    {
        // Arrange
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("test-tenant");

        var repository = new DcbRepository(
            _eventStore!,
            metadataProvider,
            tenantProvider,
            DefaultLawnDartOptions(),
            null,
            null,
            null);

        // Append events with different tag combinations
        await repository.AppendWithContextAsync(
            new[] { new TestDcbEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = 10 } },
            new[] { "entity:A", "type:order" });

        await repository.AppendWithContextAsync(
            new[] { new TestDcbEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = 20 } },
            new[] { "entity:B", "type:order" });

        await repository.AppendWithContextAsync(
            new[] { new TestDcbEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = 30 } },
            new[] { "entity:A", "type:payment" });

        // Act - Query by single entity
        var stateA = await repository.GetStateAsync<TestDcbState>(new[] { "entity:A" });
        var stateB = await repository.GetStateAsync<TestDcbState>(new[] { "entity:B" });

        // Assert
        Assert.Equal(40, stateA.TotalValue); // 10 + 30
        Assert.Equal(20, stateB.TotalValue); // 20 only
    }

    [Fact]
    public async Task DcbOperations_WithTenantTags_IsolatesData()
    {
        // Arrange
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("tenant-1");

        var repository = new DcbRepository(
            _eventStore!,
            metadataProvider,
            tenantProvider,
            DefaultLawnDartOptions(),
            null,
            null,
            null);

        // Append events for tenant-1
        await repository.AppendWithContextAsync(
            new[] { new TestDcbEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = 100 } },
            new[] { "tenant:tenant-1", "order:order-1" });

        // Append events for tenant-2
        await repository.AppendWithContextAsync(
            new[] { new TestDcbEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = 200 } },
            new[] { "tenant:tenant-2", "order:order-1" });

        // Act - Query with tenant-1 tags
        var stateTenant1 = await repository.GetStateAsync<TestDcbState>(new[] { "tenant:tenant-1", "order:order-1" });

        // Query with tenant-2 tags
        var stateTenant2 = await repository.GetStateAsync<TestDcbState>(new[] { "tenant:tenant-2", "order:order-1" });

        // Assert
        Assert.Equal(100, stateTenant1.TotalValue);
        Assert.Equal(200, stateTenant2.TotalValue);
    }

    [Fact]
    public async Task GetLastSequencePositionAsync_UsesStoreMax()
    {
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("test-tenant");
        var repository = new DcbRepository(
            _eventStore!,
            metadataProvider,
            tenantProvider,
            DefaultLawnDartOptions(),
            null,
            null,
            null);

        var tags = new[] { "order:max-seq" };
        await repository.AppendWithContextAsync(
            new[] { new TestDcbEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = 1 } },
            tags);
        await repository.AppendWithContextAsync(
            new[] { new TestDcbEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = 2 } },
            tags);

        var fromRepo = await repository.GetLastSequencePositionAsync(tags);
        var fromStore = await _eventStore!.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags(tags)));
        Assert.Equal(fromStore, fromRepo);
        Assert.True(fromRepo > 0);
    }

    // Test types
    [EventTypeName("dcb-sql-server-integration-tests.test-dcb-event")]
    private record TestDcbEvent : IEvent
    {
        public Guid Id { get; init; }
        public DateTime Timestamp { get; init; }
        public int Value { get; init; }
    }

    private class TestDcbState : IState
    {
        public int TotalValue { get; private set; }
        public int EventCount { get; private set; }

        public void Apply(TestDcbEvent @event)
        {
            TotalValue += @event.Value;
            EventCount++;
        }
    }
}
