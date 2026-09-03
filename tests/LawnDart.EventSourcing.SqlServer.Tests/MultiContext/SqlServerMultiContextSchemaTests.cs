using Microsoft.Data.SqlClient;
using LawnDart.EventSourcing.SqlServer.EventStore;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.MultiContext;

/// <summary>
/// Verifies that <see cref="SqlServerEventStore.InitializeSchemaAsync"/> creates the correct
/// SQL Server schema and places all objects (tables, sequences, indexes) inside it.
/// </summary>
[Trait("Category", "Integration")]
public class SqlServerMultiContextSchemaTests : IAsyncLifetime
{
    private readonly MsSqlContainer _container;
    private string _connectionString = string.Empty;

    public SqlServerMultiContextSchemaTests()
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
    public async Task DefaultSchemaName_IsCompatibleWithDbo()
    {
        var options = new SqlServerEventStoreOptions
        {
            RequireTenantId = false,
            SchemaName      = "dbo",
            ContextName     = "default"
        };
        var store = CreateStore($"Events_{Guid.NewGuid():N}", options);
        await store.InitializeSchemaAsync();

        Assert.True(await SchemaExistsAsync("dbo"));
    }

    [Fact]
    public async Task CustomSchemaName_IsCreatedAutomatically()
    {
        const string schema = "ordering";
        var options = new SqlServerEventStoreOptions
        {
            RequireTenantId = false,
            SchemaName      = schema,
            ContextName     = schema
        };
        var store = CreateStore($"Events_{Guid.NewGuid():N}", options);
        await store.InitializeSchemaAsync();

        Assert.True(await SchemaExistsAsync(schema));
    }

    [Fact]
    public async Task TwoContexts_CreateIndependentSchemas()
    {
        var optOrdering = new SqlServerEventStoreOptions { RequireTenantId = false, SchemaName = "ordering", ContextName = "ordering" };
        var optCatalog  = new SqlServerEventStoreOptions { RequireTenantId = false, SchemaName = "catalog",  ContextName = "catalog"  };

        var storeOrdering = CreateStore($"Events_{Guid.NewGuid():N}", optOrdering);
        var storeCatalog  = CreateStore($"Events_{Guid.NewGuid():N}", optCatalog);

        await storeOrdering.InitializeSchemaAsync();
        await storeCatalog.InitializeSchemaAsync();

        Assert.True(await SchemaExistsAsync("ordering"));
        Assert.True(await SchemaExistsAsync("catalog"));
    }

    [Fact]
    public async Task EventsTable_IsCreatedInConfiguredSchema()
    {
        const string schema    = "billing";
        const string tableName = "BillingEvents";
        var options = new SqlServerEventStoreOptions
        {
            RequireTenantId    = false,
            SchemaName         = schema,
            ContextName        = "billing",
            EventStoreTableName = tableName
        };
        var store = CreateStore(tableName, options);
        await store.InitializeSchemaAsync();

        Assert.True(await TableExistsAsync(schema, tableName));
        Assert.False(await TableExistsAsync("dbo", tableName));
    }

    [Fact]
    public async Task SequenceObject_IsCreatedInConfiguredSchema()
    {
        const string schema = "inventory";
        var options = new SqlServerEventStoreOptions { RequireTenantId = false, SchemaName = schema, ContextName = "inventory" };
        var store = CreateStore($"Events_{Guid.NewGuid():N}", options);
        await store.InitializeSchemaAsync();

        Assert.True(await SequenceExistsAsync(schema, "EventSequencePosition"));
        Assert.False(await SequenceExistsAsync("dbo", $"inventory_EventSequencePosition"));
    }

    [Fact]
    public async Task InitializeSchemaAsync_CreatesSnapshotTables()
    {
        const string schema = "snapshots";
        var options = new SqlServerEventStoreOptions
        {
            RequireTenantId = false,
            SchemaName = schema,
            ContextName = "snapshots"
        };
        var eventsTable = $"Events_{Guid.NewGuid():N}";
        var store = CreateStore(eventsTable, options);
        await store.InitializeSchemaAsync();
        await store.InitializeSchemaAsync();

        Assert.True(await TableExistsAsync(schema, $"DcbSnapshots_{eventsTable}"));
        Assert.True(await TableExistsAsync(schema, $"EventSnapshots_{eventsTable}"));
    }

    [Fact]
    public async Task InitializeSchemaAsync_IsIdempotent_ForCustomSchema()
    {
        const string schema = "payments";
        var options = new SqlServerEventStoreOptions { RequireTenantId = false, SchemaName = schema, ContextName = "payments" };
        var tableName = $"Events_{Guid.NewGuid():N}";
        var store = CreateStore(tableName, options);

        // Should not throw on repeated calls
        await store.InitializeSchemaAsync();
        await store.InitializeSchemaAsync();

        Assert.True(await TableExistsAsync(schema, tableName));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private SqlServerEventStore CreateStore(string tableName, SqlServerEventStoreOptions options) =>
        new(_connectionString, null, tableName, $"Streams_{Guid.NewGuid():N}", false, options, null, null);

    private async Task<bool> SchemaExistsAsync(string schemaName)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(
            "SELECT COUNT(1) FROM sys.schemas WHERE name = @name", conn);
        cmd.Parameters.AddWithValue("@name", schemaName);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync()) > 0;
    }

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

    private async Task<bool> SequenceExistsAsync(string schemaName, string sequenceName)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(
            "SELECT COUNT(1) FROM sys.sequences " +
            "WHERE name = @name AND schema_id = SCHEMA_ID(@schema)", conn);
        cmd.Parameters.AddWithValue("@name", sequenceName);
        cmd.Parameters.AddWithValue("@schema", schemaName);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync()) > 0;
    }
}
