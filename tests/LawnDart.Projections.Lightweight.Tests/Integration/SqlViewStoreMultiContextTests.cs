using Microsoft.Data.SqlClient;
using LawnDart.Projections.Storage;
using Testcontainers.MsSql;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Verifies that <see cref="SqlViewStore"/> creates and uses schema-qualified tables,
/// and that two view stores for different contexts do not share state.
/// </summary>
[Trait("Category", "Integration")]
public class SqlViewStoreMultiContextTests : IAsyncLifetime
{
    private readonly MsSqlContainer _container;
    private string _connectionString = string.Empty;

    public SqlViewStoreMultiContextTests()
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

    [Fact]
    public async Task InitializeSchemaAsync_CreatesProjectionViewsInConfiguredSchema()
    {
        var store = new SqlViewStore(_connectionString, schemaName: "ordering");

        await store.InitializeSchemaAsync();

        Assert.True(await TableExistsAsync("ordering", "ProjectionViews"));
        Assert.False(await TableExistsAsync("dbo", "ProjectionViews"));
    }

    [Fact]
    public async Task TwoViewStores_UseIndependentTables()
    {
        var storeOrdering = new SqlViewStore(_connectionString, schemaName: "ordering");
        var storeContext2 = new SqlViewStore(_connectionString, schemaName: "context2");

        await storeOrdering.InitializeSchemaAsync();
        await storeContext2.InitializeSchemaAsync();

        Assert.True(await TableExistsAsync("ordering", "ProjectionViews"));
        Assert.True(await TableExistsAsync("context2", "ProjectionViews"));
    }

    [Fact]
    public async Task SaveGetDelete_InOneSchema_DoesNotAffectOther()
    {
        var storeOrdering = new SqlViewStore(_connectionString, schemaName: "ordering");
        var storeContext2 = new SqlViewStore(_connectionString, schemaName: "context2");

        await storeOrdering.InitializeSchemaAsync();
        await storeContext2.InitializeSchemaAsync();

        await storeOrdering.SaveViewAsync("OrderSummary:v1", "instance-1", "{\"n\":1}", checkpoint: 10);

        var fromOrdering = await storeOrdering.GetViewAsync("OrderSummary:v1", "instance-1");
        Assert.Equal("{\"n\":1}", fromOrdering);

        var fromContext2 = await storeContext2.GetViewAsync("OrderSummary:v1", "instance-1");
        Assert.Null(fromContext2);

        await storeOrdering.DeleteViewAsync("OrderSummary:v1", "instance-1");
        Assert.Null(await storeOrdering.GetViewAsync("OrderSummary:v1", "instance-1"));
    }

    [Fact]
    public async Task DefaultCtor_CreatesDboProjectionViews()
    {
        var store = new SqlViewStore(_connectionString);

        await store.InitializeSchemaAsync();

        Assert.True(await TableExistsAsync("dbo", "ProjectionViews"));
    }

    [Fact]
    public async Task WhitespaceSchemaName_DefaultsToDbo()
    {
        var store = new SqlViewStore(_connectionString, schemaName: "   ");

        await store.InitializeSchemaAsync();

        Assert.True(await TableExistsAsync("dbo", "ProjectionViews"));
    }

    [Fact]
    public async Task SaveViewsAsync_BulkUpsert_RoundTripsAndUpdates()
    {
        var store = new SqlViewStore(_connectionString, schemaName: "bulkdemo");
        await store.InitializeSchemaAsync();

        var batch = Enumerable.Range(0, 12)
            .Select(i => ($"inst-{i}", $"{{\"n\":{i}}}", (long)(100 + i)))
            .ToList();

        await store.SaveViewsAsync("BulkProj:v1", batch);

        for (var i = 0; i < 12; i++)
        {
            var got = await store.GetViewWithCheckpointAsync("BulkProj:v1", $"inst-{i}");
            Assert.NotNull(got);
            Assert.Equal($"{{\"n\":{i}}}", got!.Value.ViewData);
            Assert.Equal(100 + i, got.Value.Checkpoint);
        }

        // Update subset via second bulk write
        await store.SaveViewsAsync("BulkProj:v1",
        [
            ("inst-0", "{\"n\":999}", 999),
            ("inst-11", "{\"n\":1111}", 1111)
        ]);

        var u0 = await store.GetViewWithCheckpointAsync("BulkProj:v1", "inst-0");
        var u11 = await store.GetViewWithCheckpointAsync("BulkProj:v1", "inst-11");
        Assert.Equal("{\"n\":999}", u0!.Value.ViewData);
        Assert.Equal(999, u0.Value.Checkpoint);
        Assert.Equal("{\"n\":1111}", u11!.Value.ViewData);
        Assert.Equal(1111, u11.Value.Checkpoint);

        // Untouched row remains
        var mid = await store.GetViewAsync("BulkProj:v1", "inst-5");
        Assert.Equal("{\"n\":5}", mid);
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
}
