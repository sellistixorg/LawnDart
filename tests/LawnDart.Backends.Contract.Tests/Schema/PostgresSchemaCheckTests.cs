using System.Text;
using LawnDart;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.Postgres.Outbox;
using LawnDart.EventSourcing.Postgres.Snapshots;
using LawnDart.Messaging;
using LawnDart.Messaging.Postgres;
using LawnDart.Projections.Lightweight.Postgres;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Testcontainers.PostgreSql;

namespace LawnDart.Backends.Contract.Tests;

[Trait("Category", "Integration")]
[Collection("SchemaCheck")]
public sealed class PostgresSchemaCheckTests(PostgresSchemaCheckFixture fixture)
    : IClassFixture<PostgresSchemaCheckFixture>
{
    [Fact]
    public async Task VerifySchemaAsync_MatchesAfterSetup()
    {
        var bundle = await fixture.CreateAsync("chk_match");

        AssertMatch(await bundle.Store.VerifySchemaAsync(), "1");
        AssertMatch(await bundle.Outbox.VerifySchemaAsync(), "1");
        AssertMatch(await bundle.Snapshots.VerifySchemaAsync(), null);
        AssertMatch(await bundle.Views.VerifySchemaAsync(), null);
        AssertMatch(await bundle.Checkpoints.VerifySchemaAsync(), null);
        AssertMatch(await bundle.Projections.VerifySchemaAsync(), null);
        AssertMatch(await bundle.Inbox.VerifySchemaAsync(), "1");

        var services = new ServiceCollection();
        services.AddPostgresProjectionStores("default", fixture.AdminConnectionString, bundle.Schema);
        services.AddPostgresInboxStore(fixture.AdminConnectionString, options => options.SchemaName = bundle.Schema);
        await using var provider = services.BuildServiceProvider();
        AssertMatch(await provider.VerifyPostgresProjectionStoresAsync("default"), null);
        AssertMatch(await provider.VerifyPostgresInboxStoreAsync(), "1");
    }

    [Fact]
    public async Task VerifySchemaAsync_MismatchesWhenATableIsMissing()
    {
        var bundle = await fixture.CreateAsync("chk_miss");
        await fixture.ExecuteAsync($"""DROP TABLE {LawnDart.EventSourcing.Postgres.PgIdentifier.Qualify(bundle.Schema, "stream_tags")}""");

        var result = await bundle.Store.VerifySchemaAsync();

        Assert.False(result.Matches);
        Assert.Equal("1", result.ExpectedFormat);
        Assert.Equal("1", result.FoundFormat);
        Assert.Contains("stream_tags", result.Missing);
        Assert.Throws<SchemaMismatchException>(result.ThrowIfMismatch);
    }

    [Fact]
    public async Task VerifySchemaAsync_MismatchesOnAnOlderStamp()
    {
        var bundle = await fixture.CreateAsync("chk_stamp");
        var info = LawnDart.EventSourcing.Postgres.PgIdentifier.Qualify(bundle.Schema, "schema_info");
        await fixture.ExecuteAsync($"""
            UPDATE {info} SET format = 0 WHERE component IN ('events', 'outbox', 'inbox')
            """);

        var events = await bundle.Store.VerifySchemaAsync();
        var outbox = await bundle.Outbox.VerifySchemaAsync();
        var inbox = await bundle.Inbox.VerifySchemaAsync();

        Assert.False(events.Matches);
        Assert.Equal("1", events.ExpectedFormat);
        Assert.Equal("0", events.FoundFormat);
        Assert.False(outbox.Matches);
        Assert.Equal("0", outbox.FoundFormat);
        Assert.False(inbox.Matches);
        Assert.Equal("0", inbox.FoundFormat);
    }

    [Fact]
    public async Task VerifySchemaAsync_MismatchesWhenTheFunctionBodyIsReplaced()
    {
        var bundle = await fixture.CreateAsync("chk_body");
        var quoted = LawnDart.EventSourcing.Postgres.PgIdentifier.Quote(bundle.Schema);
        var escaped = bundle.Schema.Replace("'", "''", StringComparison.Ordinal);
        await fixture.ExecuteAsync($"""
            DO $drop$
            DECLARE sig regprocedure;
            BEGIN
                FOR sig IN
                    SELECT p.oid::regprocedure
                    FROM pg_proc p
                    JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE n.nspname = '{escaped}'
                      AND p.proname IN ('append_to_stream', 'save_projection_flush')
                LOOP
                    EXECUTE 'DROP FUNCTION ' || sig::text;
                END LOOP;
            END
            $drop$;
            CREATE FUNCTION {quoted}.append_to_stream() RETURNS void LANGUAGE sql AS 'SELECT 1';
            CREATE FUNCTION {quoted}.save_projection_flush() RETURNS void LANGUAGE sql AS 'SELECT 1';
            """);

        var events = await bundle.Store.VerifySchemaAsync();
        var projections = await bundle.Projections.VerifySchemaAsync();

        Assert.False(events.Matches);
        Assert.Equal("1", events.FoundFormat);
        Assert.Contains("append_to_stream body", events.Missing);
        Assert.False(projections.Matches);
        Assert.Contains("save_projection_flush body", projections.Missing);
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
        await bundle.Projections.VerifySchemaAsync();
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

        AssertMatch(await read.Store.VerifySchemaAsync(), "1");
        AssertMatch(await read.Outbox.VerifySchemaAsync(), "1");
        AssertMatch(await read.Snapshots.VerifySchemaAsync(), null);
        AssertMatch(await read.Views.VerifySchemaAsync(), null);
        AssertMatch(await read.Checkpoints.VerifySchemaAsync(), null);
        AssertMatch(await read.Projections.VerifySchemaAsync(), null);
        AssertMatch(await read.Inbox.VerifySchemaAsync(), "1");

        var after = await fixture.CatalogAsync(bundle.Schema);
        Assert.Equal(before, after);

        await using var connection = new NpgsqlConnection(reader);
        await connection.OpenAsync();
        var info = LawnDart.EventSourcing.Postgres.PgIdentifier.Qualify(bundle.Schema, "schema_info");
        await using var write = new NpgsqlCommand(
            $"UPDATE {info} SET format = format WHERE component = 'events'",
            connection);
        var denied = await Assert.ThrowsAsync<PostgresException>(() => write.ExecuteNonQueryAsync());
        Assert.Equal("42501", denied.SqlState);
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
}

public sealed class PostgresSchemaCheckFixture : IAsyncLifetime
{
    private PostgreSqlContainer? _container;

    public string AdminConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder(PostgresTestImage.Current)
            .WithDatabase("lawndart")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .Build();
        await _container.StartAsync();
        AdminConnectionString = _container.GetConnectionString();
        await ExecuteAsync("""
            DO $$ BEGIN
                CREATE ROLE lawndart_schema_reader LOGIN PASSWORD 'lawndart_schema_reader'
                    NOSUPERUSER NOCREATEDB NOCREATEROLE;
            EXCEPTION
                WHEN duplicate_object THEN NULL;
            END $$;
            GRANT CONNECT ON DATABASE lawndart TO lawndart_schema_reader;
            """);
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }

    public PgBundle Open(string connectionString, string schema)
    {
        var options = new PostgresEventStoreOptions
        {
            ConnectionString = connectionString,
            SchemaName = schema,
            RequireTenantId = false
        };
        var projectionOptions = new PostgresProjectionStoreOptions
        {
            ConnectionString = connectionString,
            SchemaName = schema
        };
        var views = new PostgresViewStore(projectionOptions);
        var checkpoints = new PostgresCheckpointStore(projectionOptions, views);
        return new PgBundle(
            schema,
            new PostgresEventStore(connectionString, options: options),
            new PostgresOutboxWriter(connectionString, schemaName: schema),
            new PostgresSnapshotStore(options),
            views,
            checkpoints,
            new PostgresProjectionStore(views, checkpoints),
            new PostgresInboxStore(
                Options.Create(new PostgresInboxStoreOptions
                {
                    ConnectionString = connectionString,
                    SchemaName = schema
                }),
                Options.Create(new MessagingOptions())));
    }

    public async Task<PgBundle> CreateAsync(string schema)
    {
        var bundle = Open(AdminConnectionString, schema);
        await bundle.Store.InitializeSchemaAsync();
        await bundle.Outbox.InitializeSchemaAsync();
        await bundle.Snapshots.InitializeSchemaAsync();
        await bundle.Projections.InitializeSchemaAsync();
        await bundle.Inbox.InitializeSchemaAsync();
        return bundle;
    }

    public async Task<string> ReaderConnectionAsync(string schema)
    {
        var quoted = LawnDart.EventSourcing.Postgres.PgIdentifier.Quote(schema);
        await ExecuteAsync($"""
            GRANT USAGE ON SCHEMA {quoted} TO lawndart_schema_reader;
            GRANT SELECT ON {quoted}.schema_info TO lawndart_schema_reader;
            """);
        var builder = new NpgsqlConnectionStringBuilder(AdminConnectionString)
        {
            Username = "lawndart_schema_reader",
            Password = "lawndart_schema_reader"
        };
        return builder.ConnectionString;
    }

    public async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<string> CatalogAsync(string schema)
    {
        await using var connection = new NpgsqlConnection(AdminConnectionString);
        await connection.OpenAsync();
        var builder = new StringBuilder();
        await AppendAsync(connection, builder, """
            SELECT c.relkind::text || ' ' || c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = @schema
            ORDER BY 1
            """, schema);
        await AppendAsync(connection, builder, """
            SELECT p.proname || ' ' || md5(p.prosrc)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = @schema
            ORDER BY 1
            """, schema);
        await AppendAsync(connection, builder, """
            SELECT t.tgname || ' ' || c.relname
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = @schema AND NOT t.tgisinternal
            ORDER BY 1
            """, schema);
        await AppendAsync(connection, builder, """
            SELECT typname
            FROM pg_type t
            JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = @schema
            ORDER BY 1
            """, schema);
        var info = LawnDart.EventSourcing.Postgres.PgIdentifier.Qualify(schema, "schema_info");
        await using (var command = new NpgsqlCommand($"""
            SELECT component || ' ' || format::text || ' ' || partition_count::text
                || ' ' || partitioning::text || ' ' || updated_at::text
            FROM {info}
            ORDER BY component
            """, connection))
        await using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                builder.Append(reader.GetString(0));
                builder.Append('\n');
            }
        }

        return builder.ToString();
    }

    private static async Task AppendAsync(
        NpgsqlConnection connection,
        StringBuilder builder,
        string sql,
        string schema)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("schema", schema);
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            builder.Append(reader.GetString(0));
            builder.Append('\n');
        }
    }
}

public sealed record PgBundle(
    string Schema,
    PostgresEventStore Store,
    PostgresOutboxWriter Outbox,
    PostgresSnapshotStore Snapshots,
    PostgresViewStore Views,
    PostgresCheckpointStore Checkpoints,
    PostgresProjectionStore Projections,
    PostgresInboxStore Inbox);

[CollectionDefinition("SchemaCheck", DisableParallelization = true)]
public sealed class SchemaCheckCollection;
