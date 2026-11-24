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

    [Theory]
    [InlineData("1")]
    [InlineData("2")]
    public async Task Older_outbox_format_is_upgraded_in_place_and_keeps_rows(string format)
    {
        var table = "OutboxFormat" + format;
        var claimedUntil = format == "1" ? "" : ",\n                ClaimedUntil DATETIME2 NULL";
        await using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            await using var create = new SqlCommand($@"
                CREATE TABLE [dbo].[{table}] (
                    Id UNIQUEIDENTIFIER PRIMARY KEY,
                    EventType NVARCHAR(500) NOT NULL,
                    SchemaVersion INT NOT NULL,
                    CodecId TINYINT NOT NULL,
                    Payload VARBINARY(MAX) NOT NULL,
                    Metadata NVARCHAR(MAX) NOT NULL,
                    CreatedAt DATETIME2 NOT NULL,
                    ProcessedAt DATETIME2 NULL,
                    Attempts INT NOT NULL,
                    LastError NVARCHAR(MAX) NULL,
                    LastAttemptAt DATETIME2 NULL,
                    StreamId NVARCHAR(500) NOT NULL,
                    SequencePosition BIGINT NOT NULL,
                    DeadLetteredAt DATETIME2 NULL{claimedUntil}
                );
                EXEC sys.sp_addextendedproperty
                    @name = N'LawnDart_OutboxSchemaFormat', @value = N'{format}',
                    @level0type = N'SCHEMA', @level0name = N'dbo',
                    @level1type = N'TABLE', @level1name = N'{table}';
                INSERT INTO [dbo].[{table}] (
                    Id, EventType, SchemaVersion, CodecId, Payload, Metadata, CreatedAt,
                    Attempts, StreamId, SequencePosition)
                VALUES (NEWID(), N'TestEvent', 1, 1, 0x7B7D, N'{{}}', SYSUTCDATETIME(), 0, N'stream-1', 1);",
                connection);
            await create.ExecuteNonQueryAsync();
        }

        var writer = new SqlServerOutboxWriter(
            _connectionString!, table, NullLogger<SqlServerOutboxWriter>.Instance, claimLease: TimeSpan.Zero);
        await writer.InitializeSchemaAsync();

        var check = await writer.VerifySchemaAsync();
        Assert.Equal(SqlServerOutboxWriter.CurrentSchemaFormat, check.FoundFormat);
        var row = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(0, row.PublishedTo);

        await writer.RecordFailureAsync(row.Id, "second publisher down", publishedTo: 0b01);
        Assert.Equal(0b01, Assert.Single(await writer.GetUnprocessedAsync(10)).PublishedTo);

        await writer.InitializeSchemaAsync();
        Assert.Equal(0b01, Assert.Single(await writer.GetUnprocessedAsync(10)).PublishedTo);
    }

    [Fact]
    public async Task Fresh_outbox_table_is_format_3_with_PublishedTo()
    {
        var check = await _writer!.VerifySchemaAsync();
        Assert.Equal("3", check.FoundFormat);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var cmd = new SqlCommand(
            """
            SELECT t.name
            FROM sys.columns c
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(N'[dbo].[Outbox]') AND c.name = N'PublishedTo' AND c.is_nullable = 0
            """,
            connection);
        Assert.Equal("bigint", (string?)await cmd.ExecuteScalarAsync());
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
