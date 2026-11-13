using Microsoft.Data.SqlClient;
using LawnDart;
using LawnDart.EventSourcing.Serialization;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventSourcing.SqlServer.Outbox;
using LawnDart.EventStore;
using LawnDart.Outbox;
using LawnDart.Publishing;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.MultiContext;

/// <summary>
/// Verifies that <see cref="SqlServerOutboxWriter"/> creates and uses schema-qualified outbox
/// tables, and that two writers for different contexts do not share outbox state.
/// </summary>
[Trait("Category", "Integration")]
public class SqlServerMultiContextOutboxTests : IAsyncLifetime
{
    private MsSqlContainer? _container;
    private string _connectionString = string.Empty;

    public async Task InitializeAsync()
    {
        _container = await MsSqlContainerStartup.StartAsync(() =>
            new MsSqlBuilder(MsSqlTestImage.Server2025)
                .WithPassword("Test123!")
                .Build());
        _connectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }

    // -------------------------------------------------------------------------

    [Fact]
    public async Task InitializeSchemaAsync_CreatesOutboxTableInConfiguredSchema()
    {
        var writer = new SqlServerOutboxWriter(_connectionString, "Outbox", null, "ordering");
        await writer.InitializeSchemaAsync();

        Assert.True(await TableExistsAsync("ordering", "Outbox"));
        Assert.False(await TableExistsAsync("EventStore", "Outbox"));
    }

    [Fact]
    public async Task TwoOutboxWriters_UseIndependentTables()
    {
        var writerOrdering = new SqlServerOutboxWriter(_connectionString, "Outbox", null, "ordering");
        var writerContext2 = new SqlServerOutboxWriter(_connectionString, "Outbox", null, "context2");

        await writerOrdering.InitializeSchemaAsync();
        await writerContext2.InitializeSchemaAsync();

        Assert.True(await TableExistsAsync("ordering", "Outbox"));
        Assert.True(await TableExistsAsync("context2", "Outbox"));
    }

    [Fact]
    public async Task TwoContexts_WriteOnlyTheirDeclaredNotices()
    {
        var orderingSchema = "o" + Guid.NewGuid().ToString("N");
        var catalogSchema = "c" + Guid.NewGuid().ToString("N");
        var ordering = await OpenDeclaredStoreAsync(
            orderingSchema,
            [typeof(OrderNoted), typeof(OrderingInternal)],
            events => events.Publish<OrderNoted>());
        var catalog = await OpenDeclaredStoreAsync(
            catalogSchema,
            [typeof(ProductNoted), typeof(CatalogInternal)],
            events => events.Publish<ProductNoted>());

        var noted = new OrderNoted(Guid.NewGuid(), DateTime.UtcNow, "o-1");
        await ordering.Store.AppendAsync(Stream("Order"), [
            noted,
            new OrderingInternal(Guid.NewGuid(), DateTime.UtcNow)
        ]);
        var product = new ProductNoted(Guid.NewGuid(), DateTime.UtcNow, "sku-1");
        await catalog.Store.AppendAsync(Stream("Product"), [
            product,
            new CatalogInternal(Guid.NewGuid(), DateTime.UtcNow)
        ]);

        var orderingRow = Assert.Single(await ordering.Outbox.GetUnprocessedAsync(10));
        var catalogRow = Assert.Single(await catalog.Outbox.GetUnprocessedAsync(10));
        Assert.Equal(noted.Id, orderingRow.Id);
        Assert.Equal("ordering.order-noted", orderingRow.EventType);
        Assert.Equal(product.Id, catalogRow.Id);
        Assert.Equal("catalog.product-noted", catalogRow.EventType);
    }

    private async Task<(SqlServerEventStore Store, IOutboxWriter Outbox)> OpenDeclaredStoreAsync(
        string schema,
        Type[] eventTypes,
        Action<PublishedEventsBuilder> declare)
    {
        var catalog = EventTypeCatalog.Materialize(eventTypes);
        var map = PublishedEventMap.Create(schema, catalog, declare);
        var options = new SqlServerEventStoreOptions
        {
            ConnectionString = _connectionString,
            SchemaName = schema,
            ContextName = schema,
            RequireTenantId = false,
            EnableOutbox = true,
            OutboxTableName = "Outbox"
        };
        var outbox = new SqlServerOutboxWriter(_connectionString, "Outbox", null, schema);
        await outbox.InitializeSchemaAsync();
        var store = new SqlServerEventStore(
            _connectionString,
            options: options,
            session: new EventSession(new JsonEventSerializer(), catalog),
            publishedEvents: map);
        await store.InitializeSchemaAsync();
        return (store, outbox);
    }

    private static string Stream(string aggregate) => "acme:" + aggregate + ":" + Guid.NewGuid().ToString("N");




    private static OutboxMessage DeadLetterCandidate(Guid id, long sequence)
        => new()
        {
            Id = id,
            EventType = "OrderPlaced",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            Attempts = 4,
            StreamId = "no-tenant:Order:order-reset",
            SequencePosition = sequence
        };

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    [EventTypeName("ordering.order-noted")]
    public sealed record OrderNoted(Guid Id, DateTime Timestamp, string OrderId) : IEvent;

    [EventTypeName("ordering.internal")]
    public sealed record OrderingInternal(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("catalog.product-noted")]
    public sealed record ProductNoted(Guid Id, DateTime Timestamp, string Sku) : IEvent;

    [EventTypeName("catalog.internal")]
    public sealed record CatalogInternal(Guid Id, DateTime Timestamp) : IEvent;

    private async Task<bool> TableExistsAsync(string schemaName, string tableName)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(
            "SELECT COUNT(1) FROM sys.objects " +
            "WHERE object_id = OBJECT_ID(N'[' + @schema + '].[' + @table + ']') AND type = 'U'", conn);
        cmd.Parameters.AddWithValue("@schema", schemaName);
        cmd.Parameters.AddWithValue("@table", tableName);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync()) > 0;
    }
}
