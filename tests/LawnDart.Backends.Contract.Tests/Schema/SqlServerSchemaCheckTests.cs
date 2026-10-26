using System.Text;
using LawnDart;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventSourcing.SqlServer.Outbox;
using LawnDart.EventSourcing.SqlServer.Snapshots;
using LawnDart.Messaging;
using LawnDart.Messaging.SqlServer;
using LawnDart.Projections.Lightweight.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Testcontainers.MsSql;

namespace LawnDart.Backends.Contract.Tests;

[Trait("Category", "Integration")]
[Collection("SchemaCheck")]
public sealed class SqlServerSchemaCheckTests(SqlServerSchemaCheckFixture fixture)
    : IClassFixture<SqlServerSchemaCheckFixture>
{
    [Fact]
    public async Task VerifySchemaAsync_MatchesAfterSetup()
    {
        var bundle = await fixture.CreateAsync("chk_match");

        AssertMatch(await bundle.Store.VerifySchemaAsync(), "4");
        AssertMatch(await bundle.Outbox.VerifySchemaAsync(), "2");
        AssertMatch(await bundle.Snapshots.VerifySchemaAsync(), null);
        AssertMatch(await bundle.Views.VerifySchemaAsync(), null);
        AssertMatch(await bundle.Checkpoints.VerifySchemaAsync(), null);
        AssertMatch(await bundle.Inbox.VerifySchemaAsync(), "2");

        var services = new ServiceCollection();
        services.AddSqlProjectionStores("default", fixture.AdminConnectionString, bundle.Schema);
        services.AddSqlInboxStore(fixture.AdminConnectionString, options => options.SchemaName = bundle.Schema);
        await using var provider = services.BuildServiceProvider();
        AssertMatch(await provider.VerifySqlProjectionStoresAsync("default"), null);
        AssertMatch(await provider.VerifySqlInboxStoreAsync(), "2");
    }

    [Fact]
    public async Task VerifySchemaAsync_MismatchesWhenATableIsMissing()
    {
        var bundle = await fixture.CreateAsync("chk_miss");
        await fixture.ExecuteAsync($"DROP TABLE {Qualify(bundle.Schema, "EventTags")}");

        var result = await bundle.Store.VerifySchemaAsync();

        Assert.False(result.Matches);
        Assert.Equal("4", result.ExpectedFormat);
        Assert.Equal("4", result.FoundFormat);
        Assert.Contains("EventTags", result.Missing);
        Assert.Throws<SchemaMismatchException>(result.ThrowIfMismatch);
    }

    [Fact]
    public async Task VerifySchemaAsync_MismatchesWhenAKeyIndexOrColumnIsMissing()
    {
        var bundle = await fixture.CreateAsync("chk_idx");
        var events = Qualify(bundle.Schema, "Events");
        var streams = Qualify(bundle.Schema, "Streams");
        var tags = Qualify(bundle.Schema, "EventTags");
        await fixture.ExecuteAsync($"""
            DROP INDEX [UX_EventId] ON {events};
            ALTER TABLE {tags} DROP CONSTRAINT [FK_EventTags_Events];
            DROP INDEX [UX_SequencePosition] ON {events};
            DROP INDEX [UX_StreamIdBin] ON {streams};
            IF EXISTS (
                SELECT 1
                FROM sys.indexes i
                INNER JOIN sys.tables t ON t.object_id = i.object_id
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE i.name = N'IX_AggregateType' AND s.name = N'{bundle.Schema}' AND t.name = N'Streams')
                DROP INDEX [IX_AggregateType] ON {streams};
            ALTER TABLE {streams} DROP COLUMN [StreamIdBin];
            """);

        var result = await bundle.Store.VerifySchemaAsync();

        Assert.False(result.Matches);
        Assert.Contains("UX_EventId", result.Missing);
        Assert.Contains("UX_SequencePosition", result.Missing);
        Assert.Contains("UX_StreamIdBin", result.Missing);
        Assert.Contains("StreamIdBin", result.Missing);
    }

    [Fact]
    public async Task VerifySchemaAsync_ThrowsWhenTheLoginCannotReadTheSchemaDefinition()
    {
        var bundle = await fixture.CreateAsync("chk_perm");
        await fixture.ExecuteAsync("""
            IF SUSER_ID(N'lawndart_schema_none') IS NULL
                CREATE LOGIN lawndart_schema_none WITH PASSWORD = N'LawnDart_None1a', CHECK_POLICY = OFF;
            IF DATABASE_PRINCIPAL_ID(N'lawndart_schema_none') IS NULL
                CREATE USER lawndart_schema_none FOR LOGIN lawndart_schema_none;
            IF SUSER_ID(N'lawndart_schema_select') IS NULL
                CREATE LOGIN lawndart_schema_select WITH PASSWORD = N'LawnDart_Select1a', CHECK_POLICY = OFF;
            IF DATABASE_PRINCIPAL_ID(N'lawndart_schema_select') IS NULL
                CREATE USER lawndart_schema_select FOR LOGIN lawndart_schema_select;
            ALTER ROLE db_datareader ADD MEMBER lawndart_schema_select;
            """);

        var none = fixture.Open(fixture.ConnectionFor("lawndart_schema_none", "LawnDart_None1a"), bundle.Schema);
        var select = fixture.Open(fixture.ConnectionFor("lawndart_schema_select", "LawnDart_Select1a"), bundle.Schema);

        var noneError = await Assert.ThrowsAsync<InvalidOperationException>(() => none.Store.VerifySchemaAsync());
        var selectError = await Assert.ThrowsAsync<InvalidOperationException>(() => select.Store.VerifySchemaAsync());

        Assert.Contains("lacks permission to read the schema definition", noneError.Message, StringComparison.Ordinal);
        Assert.Contains("lacks permission to read the schema definition", selectError.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("EventSequencePosition", selectError.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task VerifySchemaAsync_SaysWhenTheSchemaDoesNotExist()
    {
        var store = fixture.Open(fixture.AdminConnectionString, "chk_absent");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => store.Store.VerifySchemaAsync());

        Assert.Contains("Schema 'chk_absent' does not exist, or the login cannot read its definition.", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task VerifySchemaAsync_MismatchesOnAnOlderStamp()
    {
        var bundle = await fixture.CreateAsync("chk_stamp");
        await fixture.ExecuteAsync($"""
            EXEC sys.sp_updateextendedproperty
                @name = N'LawnDart_SchemaFormat', @value = N'2',
                @level0type = N'SCHEMA', @level0name = N'{bundle.Schema}',
                @level1type = N'TABLE', @level1name = N'Events';
            EXEC sys.sp_updateextendedproperty
                @name = N'LawnDart_OutboxSchemaFormat', @value = N'1',
                @level0type = N'SCHEMA', @level0name = N'{bundle.Schema}',
                @level1type = N'TABLE', @level1name = N'Outbox';
            EXEC sys.sp_updateextendedproperty
                @name = N'LawnDart_InboxSchemaFormat', @value = N'1',
                @level0type = N'SCHEMA', @level0name = N'{bundle.Schema}',
                @level1type = N'TABLE', @level1name = N'Inbox';
            """);

        var events = await bundle.Store.VerifySchemaAsync();
        var outbox = await bundle.Outbox.VerifySchemaAsync();
        var inbox = await bundle.Inbox.VerifySchemaAsync();

        Assert.False(events.Matches);
        Assert.Equal("4", events.ExpectedFormat);
        Assert.Equal("2", events.FoundFormat);
        Assert.False(outbox.Matches);
        Assert.Equal("2", outbox.ExpectedFormat);
        Assert.Equal("1", outbox.FoundFormat);
        Assert.False(inbox.Matches);
        Assert.Equal("2", inbox.ExpectedFormat);
        Assert.Equal("1", inbox.FoundFormat);
    }

    [Fact]
    public async Task VerifySchemaAsync_DoesNotChangeTheCatalog()
    {
        var bundle = await fixture.CreateAsync("chk_still");
        var before = await fixture.CatalogAsync(bundle.Schema);

        await bundle.Store.VerifySchemaAsync();
        await bundle.Outbox.VerifySchemaAsync();
        await bundle.Snapshots.VerifySchemaAsync();
        await bundle.Views.VerifySchemaAsync();
        await bundle.Checkpoints.VerifySchemaAsync();
        await bundle.Inbox.VerifySchemaAsync();

        var after = await fixture.CatalogAsync(bundle.Schema);
        Assert.Equal(before, after);
    }

    [Fact]
    public async Task VerifySchemaAsync_WorksForAReadOnlyLogin()
    {
        var bundle = await fixture.CreateAsync("chk_read");
        var reader = await fixture.ReaderConnectionAsync(bundle.Schema);
        var read = fixture.Open(reader, bundle.Schema);
        var before = await fixture.CatalogAsync(bundle.Schema);

        AssertMatch(await read.Store.VerifySchemaAsync(), "4");
        AssertMatch(await read.Outbox.VerifySchemaAsync(), "2");
        AssertMatch(await read.Snapshots.VerifySchemaAsync(), null);
        AssertMatch(await read.Views.VerifySchemaAsync(), null);
        AssertMatch(await read.Checkpoints.VerifySchemaAsync(), null);
        AssertMatch(await read.Inbox.VerifySchemaAsync(), "2");

        var after = await fixture.CatalogAsync(bundle.Schema);
        Assert.Equal(before, after);

        await using var connection = new SqlConnection(reader);
        await connection.OpenAsync();
        await using var write = new SqlCommand(
            $"UPDATE {Qualify(bundle.Schema, "Events")} SET [StreamId] = [StreamId]",
            connection);
        await Assert.ThrowsAsync<SqlException>(() => write.ExecuteNonQueryAsync());
    }

    private static void AssertMatch(SchemaCheckResult result, string? format)
    {
        Assert.True(
            result.Matches,
            $"expected {format}, found {result.FoundFormat}, missing {string.Join(", ", result.Missing)}");
        Assert.Equal(format, result.ExpectedFormat);
        Assert.Equal(format, result.FoundFormat);
        Assert.Empty(result.Missing);
        result.ThrowIfMismatch();
    }

    private static string Qualify(string schema, string table)
        => "[" + schema.Replace("]", "]]", StringComparison.Ordinal) + "].["
            + table.Replace("]", "]]", StringComparison.Ordinal) + "]";
}

public sealed class SqlServerSchemaCheckFixture : IAsyncLifetime
{
    private MsSqlContainer? _container;

    public string AdminConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        _container = await MsSqlContainerStartup.StartAsync(() =>
            new MsSqlBuilder(MsSqlTestImage.Server2025)
                .WithPassword("Test123!")
                .Build());
        AdminConnectionString = _container.GetConnectionString();
        await ExecuteAsync("""
            IF SUSER_ID(N'lawndart_schema_reader') IS NULL
                CREATE LOGIN lawndart_schema_reader WITH PASSWORD = N'LawnDart_Reader1a', CHECK_POLICY = OFF;
            IF DATABASE_PRINCIPAL_ID(N'lawndart_schema_reader') IS NULL
                CREATE USER lawndart_schema_reader FOR LOGIN lawndart_schema_reader;
            """);
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }

    public SqlBundle Open(string connectionString, string schema)
    {
        var options = new SqlServerEventStoreOptions
        {
            ConnectionString = connectionString,
            SchemaName = schema
        };
        var views = new SqlViewStore(connectionString, schemaName: schema);
        return new SqlBundle(
            schema,
            new SqlServerEventStore(connectionString, options: options),
            new SqlServerOutboxWriter(connectionString, schemaName: schema),
            new SqlServerSnapshotStore(options),
            views,
            new SqlCheckpointStore(connectionString, views, schemaName: schema),
            new SqlServerInboxStore(
                Options.Create(new SqlServerInboxStoreOptions
                {
                    ConnectionString = connectionString,
                    SchemaName = schema
                }),
                Options.Create(new MessagingOptions())));
    }

    public async Task<SqlBundle> CreateAsync(string schema)
    {
        var bundle = Open(AdminConnectionString, schema);
        await bundle.Store.InitializeSchemaAsync();
        await bundle.Outbox.InitializeSchemaAsync();
        await bundle.Snapshots.InitializeSchemaAsync();
        await bundle.Views.InitializeSchemaAsync();
        await bundle.Checkpoints.InitializeSchemaAsync();
        await bundle.Inbox.InitializeSchemaAsync();
        return bundle;
    }

    public async Task<string> ReaderConnectionAsync(string schema)
    {
        var escaped = schema.Replace("]", "]]", StringComparison.Ordinal);
        await ExecuteAsync($"GRANT VIEW DEFINITION ON SCHEMA::[{escaped}] TO lawndart_schema_reader;");
        return ConnectionFor("lawndart_schema_reader", "LawnDart_Reader1a");
    }

    public string ConnectionFor(string user, string password)
    {
        var builder = new SqlConnectionStringBuilder(AdminConnectionString)
        {
            UserID = user,
            Password = password,
            IntegratedSecurity = false
        };
        return builder.ConnectionString;
    }

    public async Task ExecuteAsync(string sql)
    {
        await using var connection = new SqlConnection(AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<string> CatalogAsync(string schema)
    {
        await using var connection = new SqlConnection(AdminConnectionString);
        await connection.OpenAsync();
        var builder = new StringBuilder();
        await AppendAsync(connection, builder, """
            SELECT CONVERT(nvarchar(20), RTRIM(o.type)) + N' ' + o.name COLLATE DATABASE_DEFAULT
                + N' ' + CONVERT(nvarchar(23), o.modify_date, 121)
            FROM sys.objects o
            INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = @schema
            ORDER BY o.type, o.name
            """, schema);
        await AppendAsync(connection, builder, """
            SELECT o.name COLLATE DATABASE_DEFAULT + N' ' + i.name COLLATE DATABASE_DEFAULT
            FROM sys.indexes i
            INNER JOIN sys.objects o ON o.object_id = i.object_id
            INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND i.name IS NOT NULL
            ORDER BY o.name, i.name
            """, schema);
        await AppendAsync(connection, builder, """
            SELECT o.name COLLATE DATABASE_DEFAULT + N' ' + ep.name COLLATE DATABASE_DEFAULT
                + N' ' + CONVERT(nvarchar(200), ep.value)
            FROM sys.extended_properties ep
            INNER JOIN sys.objects o ON o.object_id = ep.major_id
            INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND ep.minor_id = 0 AND ep.class = 1
            ORDER BY o.name, ep.name
            """, schema);
        return builder.ToString();
    }

    private static async Task AppendAsync(
        SqlConnection connection,
        StringBuilder builder,
        string sql,
        string schema)
    {
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@schema", schema);
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            builder.Append(reader.GetString(0));
            builder.Append('\n');
        }
    }
}

public sealed record SqlBundle(
    string Schema,
    SqlServerEventStore Store,
    SqlServerOutboxWriter Outbox,
    SqlServerSnapshotStore Snapshots,
    SqlViewStore Views,
    SqlCheckpointStore Checkpoints,
    SqlServerInboxStore Inbox);
