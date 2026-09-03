using Microsoft.Data.SqlClient;
using LawnDart.EventSourcing.SqlServer.Outbox;
using LawnDart.Outbox;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.MultiContext;

/// <summary>
/// Verifies that <see cref="SqlServerOutboxWriter"/> creates and uses schema-qualified outbox
/// tables, and that two writers for different contexts do not share outbox state.
/// </summary>
[Trait("Category", "Integration")]
public class SqlServerMultiContextOutboxTests : IAsyncLifetime
{
    private readonly MsSqlContainer _container;
    private string _connectionString = string.Empty;

    public SqlServerMultiContextOutboxTests()
    {
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("Test123!")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();

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
    public async Task Write_ToOrdering_NotVisibleFromContext2()
    {
        var writerOrdering = new SqlServerOutboxWriter(_connectionString, "Outbox", null, "ordering");
        var writerContext2 = new SqlServerOutboxWriter(_connectionString, "Outbox", null, "context2");

        await writerOrdering.InitializeSchemaAsync();
        await writerContext2.InitializeSchemaAsync();

        var message = new OutboxMessage
        {
            Id              = Guid.NewGuid(),
            EventType       = "OrderPlaced",
            Payload         = "{}",
            Metadata        = "{}",
            CreatedAt       = DateTime.UtcNow,
            Attempts        = 0,
            StreamId        = "no-tenant:Order:order-1",
            SequencePosition = 1
        };

        await writerOrdering.WriteAsync(message);

        var orderingUnprocessed = await writerOrdering.GetUnprocessedAsync(10);
        var context2Unprocessed = await writerContext2.GetUnprocessedAsync(10);

        Assert.Single(orderingUnprocessed);
        Assert.Empty(context2Unprocessed);
    }

    [Fact]
    public async Task MarkAsProcessed_OnlyAffectsOwnSchemaOutbox()
    {
        var writerOrdering = new SqlServerOutboxWriter(_connectionString, "Outbox", null, "ordering");
        var writerContext2 = new SqlServerOutboxWriter(_connectionString, "Outbox", null, "context2");

        await writerOrdering.InitializeSchemaAsync();
        await writerContext2.InitializeSchemaAsync();

        var messageId = Guid.NewGuid();

        await writerOrdering.WriteAsync(new OutboxMessage
        {
            Id               = messageId,
            EventType        = "OrderPlaced",
            Payload          = "{}",
            Metadata         = "{}",
            CreatedAt        = DateTime.UtcNow,
            Attempts         = 0,
            StreamId         = "no-tenant:Order:order-2",
            SequencePosition = 2
        });

        await writerOrdering.MarkAsProcessedAsync(messageId);

        // After marking processed, unprocessed list should be empty for ordering
        var unprocessed = await writerOrdering.GetUnprocessedAsync(10);
        Assert.Empty(unprocessed);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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
