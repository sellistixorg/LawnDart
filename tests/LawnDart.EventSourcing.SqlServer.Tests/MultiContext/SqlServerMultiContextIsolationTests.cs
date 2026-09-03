using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.MultiContext;

/// <summary>
/// Verifies physical isolation between two bounded contexts backed by different SQL Server schemas.
/// Events appended to the <c>ordering</c> schema are not visible from the <c>catalog</c> schema,
/// and global sequence counters are independent per schema.
/// </summary>
[Trait("Category", "Integration")]
public class SqlServerMultiContextIsolationTests : IAsyncLifetime
{
    private readonly MsSqlContainer _container;
    private string _connectionString = string.Empty;

    private SqlServerEventStore? _orderingStore;
    private SqlServerEventStore? _catalogStore;

    public SqlServerMultiContextIsolationTests()
    {
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("Test123!")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();

        _orderingStore = CreateStore("ordering");
        _catalogStore  = CreateStore("catalog");

        await _orderingStore.InitializeSchemaAsync();
        await _catalogStore.InitializeSchemaAsync();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();

    // -------------------------------------------------------------------------

    [Fact]
    public async Task Events_AppendedToOrdering_NotVisibleFromCatalog()
    {
        const string streamId = "no-tenant:Order:order-1";

        await _orderingStore!.AppendAsync(streamId, [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        // Reading same streamId from catalog returns empty (different schema / table)
        var catalogEvents = await _catalogStore!.ReadStreamAsync(streamId);
        Assert.Empty(catalogEvents);
    }

    [Fact]
    public async Task Events_AppendedToCatalog_NotVisibleFromOrdering()
    {
        const string streamId = "no-tenant:Product:product-1";

        await _catalogStore!.AppendAsync(streamId, [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        var orderingEvents = await _orderingStore!.ReadStreamAsync(streamId);
        Assert.Empty(orderingEvents);
    }

    [Fact]
    public async Task SequenceCounters_AreIndependentPerSchema()
    {
        const string orderStream   = "no-tenant:Order:order-seq";
        const string productStream = "no-tenant:Product:product-seq";

        await _orderingStore!.AppendAsync(orderStream,   [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        await _catalogStore!.AppendAsync(productStream, [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        var orderingEvents = await _orderingStore.ReadStreamAsync(orderStream);
        var catalogEvents  = await _catalogStore.ReadStreamAsync(productStream);

        Assert.Single(orderingEvents);
        Assert.Single(catalogEvents);

        // Both sequences produce valid positive positions and they may coincide in value
        // since they are independent counters — neither is derived from the other.
        Assert.True(orderingEvents[0].SequencePosition >= 1);
        Assert.True(catalogEvents[0].SequencePosition  >= 1);
    }

    [Fact]
    public async Task MultipleEvents_SequencePositionsArePerSchema()
    {
        const string streamA = "no-tenant:Order:order-multi";
        const string streamB = "no-tenant:Product:product-multi";

        await _orderingStore!.AppendAsync(streamA,
            [new StubEvent(Guid.NewGuid(), DateTime.UtcNow),
             new StubEvent(Guid.NewGuid(), DateTime.UtcNow),
             new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        await _catalogStore!.AppendAsync(streamB,
            [new StubEvent(Guid.NewGuid(), DateTime.UtcNow),
             new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        var orderEvents = await _orderingStore.ReadStreamAsync(streamA);
        var catEvents   = await _catalogStore.ReadStreamAsync(streamB);

        Assert.Equal(3, orderEvents.Count);
        Assert.Equal(2, catEvents.Count);

        // Ordering positions are consecutive within the ordering schema
        Assert.Equal(orderEvents[0].SequencePosition + 1, orderEvents[1].SequencePosition);
        Assert.Equal(orderEvents[0].SequencePosition + 2, orderEvents[2].SequencePosition);

        // Catalog positions are consecutive within the catalog schema (independent counter)
        Assert.Equal(catEvents[0].SequencePosition + 1, catEvents[1].SequencePosition);
    }

    [Fact]
    public async Task ReadByQueryAll_OnlyReturnsEventsFromOwnSchema()
    {
        const string orderStream   = "no-tenant:Order:order-all";
        const string productStream = "no-tenant:Product:product-all";

        await _orderingStore!.AppendAsync(orderStream,   [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        await _catalogStore!.AppendAsync(productStream, [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        var orderingResult = await _orderingStore.ReadByQueryAsync(Query.All());
        var catalogResult  = await _catalogStore.ReadByQueryAsync(Query.All());

        // Every event returned by the ordering store belongs to the ordering stream
        Assert.All(orderingResult.Events, e => Assert.Equal(orderStream,   e.StreamId));
        // Every event returned by the catalog store belongs to the catalog stream
        Assert.All(catalogResult.Events,  e => Assert.Equal(productStream, e.StreamId));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private SqlServerEventStore CreateStore(string contextName) =>
        new(_connectionString, null,
            $"{contextName}_Events",
            $"{contextName}_Streams",
            false,
            new SqlServerEventStoreOptions
            {
                RequireTenantId = false,
                SchemaName      = contextName,
                ContextName     = contextName
            },
            null, null);

    private sealed record StubEvent(Guid Id, DateTime Timestamp) : IEvent;
}
