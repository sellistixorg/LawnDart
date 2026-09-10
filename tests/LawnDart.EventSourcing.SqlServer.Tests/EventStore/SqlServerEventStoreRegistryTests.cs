using Microsoft.Extensions.Logging;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Testcontainers.MsSql;
using Xunit;

namespace LawnDart.EventSourcing.SqlServer.Tests.EventStore;

[Trait("Category", "Integration")]
public class SqlServerEventStoreRegistryTests : IAsyncLifetime
{
    private readonly MsSqlContainer _sqlContainer;
    private SqlServerEventStore? _eventStore;
    private string? _connectionString;
    private readonly string _tableName = $"Events_{Guid.NewGuid():N}";
    private readonly string _registryTableName = $"Streams_{Guid.NewGuid():N}";

    public SqlServerEventStoreRegistryTests()
    {
        _sqlContainer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("Test123!")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _sqlContainer.StartAsync();
        _connectionString = _sqlContainer.GetConnectionString();
        
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));
        var logger = loggerFactory.CreateLogger<SqlServerEventStore>();
        
        _eventStore = new SqlServerEventStore(_connectionString, null, _tableName, _registryTableName, true, new SqlServerEventStoreOptions { RequireTenantId = false }, null, logger);
        await _eventStore.InitializeSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        await _sqlContainer.DisposeAsync();
    }

    [Fact]
    public async Task GetStreamAsync_AfterAppend_ReturnsMetadata()
    {
        // Arrange
        var streamId = "test-tenant:Order:12345";
        var events = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var metadata = new EventMetadata { TenantId = "test-tenant" };

        // Act
        await _eventStore!.AppendAsync(streamId, events, metadata: metadata);
        var streamMetadata = await _eventStore.GetStreamAsync(streamId);

        // Assert
        Assert.NotNull(streamMetadata);
        Assert.Equal(streamId, streamMetadata!.StreamId);
        Assert.Equal("test-tenant", streamMetadata.TenantId);
        Assert.Equal("Order", streamMetadata.AggregateType);
        Assert.Equal(1, streamMetadata.CurrentVersion);
        Assert.Equal(1, streamMetadata.EventCount);
    }

    [Fact]
    public async Task GetStreamsByAggregateTypeAsync_ReturnsMatchingStreams()
    {
        // Arrange
        var metadata = new EventMetadata { TenantId = "test-tenant" };
        await _eventStore!.AppendAsync("test-tenant:Order:12345", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, metadata: metadata);
        await _eventStore.AppendAsync("test-tenant:Order:67890", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, metadata: metadata);
        await _eventStore.AppendAsync("test-tenant:Customer:11111", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, metadata: metadata);

        // Act
        var orderStreams = await _eventStore.GetStreamsByAggregateTypeAsync("Order");

        // Assert
        Assert.Equal(2, orderStreams.Count);
        Assert.All(orderStreams, s => Assert.Equal("Order", s.AggregateType));
        Assert.All(orderStreams, s => Assert.Equal("test-tenant", s.TenantId));
    }

    [Fact]
    public async Task GetStreamsByTagAsync_ReturnsMatchingStreams()
    {
        // Arrange
        var metadata = new EventMetadata { TenantId = "test-tenant" };
        await _eventStore!.AppendAsync("test-tenant:Order:12345", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, metadata: metadata, tags: new[] { "high-priority" });
        await _eventStore.AppendAsync("test-tenant:Order:67890", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, metadata: metadata, tags: new[] { "low-priority" });

        // Act
        var highPriorityStreams = await _eventStore.GetStreamsByTagAsync("high-priority");

        // Assert
        Assert.Single(highPriorityStreams);
        Assert.Contains("high-priority", highPriorityStreams[0].Tags);
        Assert.Equal("test-tenant", highPriorityStreams[0].TenantId);
    }

    [Fact]
    public async Task EnumerateStreamIdsAsync_ReturnsAllStreamIds()
    {
        // Arrange
        var metadata = new EventMetadata { TenantId = "test-tenant" };
        await _eventStore!.AppendAsync("test-tenant:Order:12345", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, metadata: metadata);
        await _eventStore.AppendAsync("test-tenant:Order:67890", new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, metadata: metadata);

        // Act
        var streamIds = await _eventStore.EnumerateStreamIdsAsync();

        // Assert
        Assert.Equal(2, streamIds.Count);
        Assert.Contains("test-tenant:Order:12345", streamIds);
        Assert.Contains("test-tenant:Order:67890", streamIds);
    }
[EventTypeName("sql-server-event-store-registry-.test-event")]

    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}


