using Xunit;
using LawnDart.EventStore;
using LawnDart.Outbox;
using LawnDart.EventSourcing.SqlServer.Outbox;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.Outbox;

[Trait("Category", "Integration")]
public class SqlServerOutboxWriterTests : IAsyncLifetime
{
    private MsSqlContainer? _container;
    private SqlServerOutboxWriter? _writer;
    private string? _connectionString;

    public async Task InitializeAsync()
    {
        _container = await MsSqlContainerStartup.StartAsync(() =>
            new MsSqlBuilder(MsSqlTestImage.Server2025).Build());
        _connectionString = _container.GetConnectionString();
        
        _writer = new SqlServerOutboxWriter(_connectionString, "Outbox", NullLogger<SqlServerOutboxWriter>.Instance);
        await _writer.InitializeSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
        }
    }












    [Fact]
    public async Task Outbox_table_layout_is_varbinary_codec_id_no_defaults()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var columns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        await using (var cmd = new SqlCommand(
            """
            SELECT c.name, t.name AS type_name
            FROM sys.columns c
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(N'[dbo].[Outbox]')
            """,
            connection))
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                columns[reader.GetString(0)] = reader.GetString(1);
        }

        Assert.False(columns.ContainsKey("ContentType"));
        Assert.Equal("varbinary", columns["Payload"]);
        Assert.Equal("tinyint", columns["CodecId"]);
        Assert.Equal("int", columns["SchemaVersion"]);

        await using (var cmd = new SqlCommand(
            """
            SELECT c.name
            FROM sys.default_constraints dc
            JOIN sys.columns c
                ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
            WHERE dc.parent_object_id = OBJECT_ID(N'[dbo].[Outbox]')
              AND c.name IN (N'SchemaVersion', N'CodecId')
            """,
            connection))
        {
            var defaults = new List<string>();
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                defaults.Add(reader.GetString(0));
            Assert.Empty(defaults);
        }
    }

    [Fact]
    public async Task Pre_stamp_outbox_table_throws_wipe_exception()
    {
        const string table = "OutboxLegacy";
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using (var create = new SqlCommand($@"
            CREATE TABLE [dbo].[{table}] (
                Id UNIQUEIDENTIFIER PRIMARY KEY,
                EventType NVARCHAR(500) NOT NULL,
                Payload NVARCHAR(MAX) NOT NULL,
                Metadata NVARCHAR(MAX) NOT NULL,
                CreatedAt DATETIME2 NOT NULL,
                StreamId NVARCHAR(500) NOT NULL,
                SequencePosition BIGINT NOT NULL
            );", connection))
        {
            await create.ExecuteNonQueryAsync();
        }

        var legacyWriter = new SqlServerOutboxWriter(
            _connectionString!,
            table,
            NullLogger<SqlServerOutboxWriter>.Instance);

        var ex = await Assert.ThrowsAsync<IncompatibleOutboxSchemaException>(
            () => legacyWriter.InitializeSchemaAsync());
        Assert.Null(ex.FoundFormat);
        Assert.Equal(SqlServerOutboxWriter.CurrentSchemaFormat, ex.RequiredFormat);
        Assert.Contains("Drop and recreate", ex.Message, StringComparison.Ordinal);
    }
}
