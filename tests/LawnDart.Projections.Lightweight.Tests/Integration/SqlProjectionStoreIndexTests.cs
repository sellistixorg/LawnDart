using Microsoft.Data.SqlClient;
using NSubstitute;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Storage;
using Testcontainers.MsSql;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Verifies projection SQL store indexes match query patterns and that
/// <see cref="SqlViewStore.InitializeSchemaAsync"/> / <see cref="SqlCheckpointStore.InitializeSchemaAsync"/>
/// upgrade legacy index sets.
/// </summary>
[Trait("Category", "Integration")]
public class SqlProjectionStoreIndexTests : IAsyncLifetime
{
    private readonly MsSqlContainer _container;
    private string _connectionString = string.Empty;

    public SqlProjectionStoreIndexTests()
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
    public async Task SqlViewStore_InitializeSchemaAsync_CreatesRequiredIndexesOnly()
    {
        var store = new SqlViewStore(_connectionString, schemaName: "idxviews");
        await store.InitializeSchemaAsync();

        Assert.True(await IndexExistsAsync("idxviews", "ProjectionViews", "IX_ProjectionType_LastUpdated"));
        Assert.False(await IndexExistsAsync("idxviews", "ProjectionViews", "IX_Checkpoint"));
        Assert.False(await IndexExistsAsync("idxviews", "ProjectionViews", "IX_ProjectionType"));
        Assert.False(await IndexExistsAsync("idxviews", "ProjectionViews", "IX_LastUpdated"));
    }

    [Fact]
    public async Task SqlCheckpointStore_InitializeSchemaAsync_DoesNotCreateObsoleteIndexes()
    {
        var viewStore = Substitute.For<IViewStore>();
        var store = new SqlCheckpointStore(_connectionString, viewStore, null, "idxckpt");
        await store.InitializeSchemaAsync();

        Assert.True(await TableExistsAsync("idxckpt", "ProjectionCheckpoints"));
        Assert.False(await IndexExistsAsync("idxckpt", "ProjectionCheckpoints", "IX_LastUpdated"));
        Assert.False(await IndexExistsAsync("idxckpt", "ProjectionCheckpoints", "IX_ProjectionType_NodeId"));
    }

    [Fact]
    public async Task SqlViewStore_InitializeSchemaAsync_UpgradesLegacyIndexes()
    {
        await using (var conn = new SqlConnection(_connectionString))
        {
            await conn.OpenAsync();
            await using var cmd = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'legacyviews')
                    EXEC('CREATE SCHEMA [legacyviews] AUTHORIZATION dbo');
                CREATE TABLE [legacyviews].[ProjectionViews] (
                    [ProjectionType] NVARCHAR(200) NOT NULL,
                    [InstanceId] NVARCHAR(500) NOT NULL,
                    [ViewData] NVARCHAR(MAX) NOT NULL,
                    [Checkpoint] BIGINT NOT NULL,
                    [LastUpdated] DATETIME2 NOT NULL,
                    CONSTRAINT [PK_ProjectionViews] PRIMARY KEY ([ProjectionType], [InstanceId])
                );
                CREATE INDEX [IX_ProjectionType] ON [legacyviews].[ProjectionViews] ([ProjectionType]);
                CREATE INDEX [IX_LastUpdated] ON [legacyviews].[ProjectionViews] ([LastUpdated]);
                CREATE INDEX [IX_Checkpoint] ON [legacyviews].[ProjectionViews] ([Checkpoint]);
            ", conn);
            await cmd.ExecuteNonQueryAsync();
        }

        var store = new SqlViewStore(_connectionString, schemaName: "legacyviews");
        await store.InitializeSchemaAsync();

        Assert.True(await IndexExistsAsync("legacyviews", "ProjectionViews", "IX_ProjectionType_LastUpdated"));
        Assert.False(await IndexExistsAsync("legacyviews", "ProjectionViews", "IX_Checkpoint"));
        Assert.False(await IndexExistsAsync("legacyviews", "ProjectionViews", "IX_ProjectionType"));
        Assert.False(await IndexExistsAsync("legacyviews", "ProjectionViews", "IX_LastUpdated"));
    }

    [Fact]
    public async Task SqlCheckpointStore_InitializeSchemaAsync_DropsLegacyIndexes()
    {
        await using (var conn = new SqlConnection(_connectionString))
        {
            await conn.OpenAsync();
            await using var cmd = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'legacyckpt')
                    EXEC('CREATE SCHEMA [legacyckpt] AUTHORIZATION dbo');
                CREATE TABLE [legacyckpt].[ProjectionCheckpoints] (
                    [ProjectionType] NVARCHAR(200) NOT NULL,
                    [NodeId] INT NOT NULL,
                    [LastSequencePosition] BIGINT NOT NULL,
                    [LastUpdated] DATETIME2 NOT NULL,
                    [TotalEventsProcessed] BIGINT NOT NULL DEFAULT 0,
                    CONSTRAINT [PK_ProjectionCheckpoints] PRIMARY KEY ([ProjectionType], [NodeId])
                );
                CREATE INDEX [IX_LastUpdated] ON [legacyckpt].[ProjectionCheckpoints] ([LastUpdated]);
                CREATE INDEX [IX_ProjectionType_NodeId] ON [legacyckpt].[ProjectionCheckpoints] ([ProjectionType], [NodeId]);
            ", conn);
            await cmd.ExecuteNonQueryAsync();
        }

        var viewStore = Substitute.For<IViewStore>();
        var store = new SqlCheckpointStore(_connectionString, viewStore, null, "legacyckpt");
        await store.InitializeSchemaAsync();

        Assert.False(await IndexExistsAsync("legacyckpt", "ProjectionCheckpoints", "IX_LastUpdated"));
        Assert.False(await IndexExistsAsync("legacyckpt", "ProjectionCheckpoints", "IX_ProjectionType_NodeId"));
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

    private async Task<bool> IndexExistsAsync(string schemaName, string tableName, string indexName)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(
            "SELECT COUNT(1) FROM sys.indexes " +
            "WHERE name = @index AND object_id = OBJECT_ID(N'[' + @schema + '].[' + @table + ']')", conn);
        cmd.Parameters.AddWithValue("@schema", schemaName);
        cmd.Parameters.AddWithValue("@table", tableName);
        cmd.Parameters.AddWithValue("@index", indexName);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync()) > 0;
    }
}
