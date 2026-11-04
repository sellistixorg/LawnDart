using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Stream tag table checks for the SQL Server and Postgres stores. These read and
/// change the table directly, so they stay with LawnDart's own backends and are not
/// in the package.
/// </summary>
public abstract class StreamTagTableContractTests : BackendContractSuite
{
    protected StreamTagTableContractTests(IFullContractBackend backend)
        : base(backend)
    {
    }

    [ContractFact]
    public async Task StreamTagTable_StaysCorrectWhenTagsChange()
    {
        SkipUnless(Cap.HasStreamTagTable, "This store has no stream tag table.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var streamId = "acme:Order:" + Guid.NewGuid().ToString("N");
        await AppendAsync(opened.Store, streamId, "alpha");
        Assert.Equal(["alpha"], TagsFor(await ReadStreamTagsAsync(opened), streamId));

        await AppendAsync(opened.Store, streamId, "beta");
        Assert.Equal(["alpha", "beta"], TagsFor(await ReadStreamTagsAsync(opened), streamId));
    }

    [ContractFact]
    public async Task StreamTagTable_DirectWrite_UpdatesTags()
    {
        SkipUnless(Cap.HasStreamTagTable, "This store has no stream tag table.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var streamId = "acme:Order:Ab" + Guid.NewGuid().ToString("N");
        await AppendAsync(opened.Store, streamId, "alpha");

        await ReplaceTagsAsync(opened, streamId, "beta");
        Assert.Equal(["beta"], TagsFor(await ReadStreamTagsAsync(opened), streamId));
        Assert.Equal(streamId, Assert.Single((await opened.Store.ListStreamsAsync(new StreamListFilter { Tag = "beta" })).Streams).StreamId);
        Assert.Empty((await opened.Store.ListStreamsAsync(new StreamListFilter { Tag = "alpha" })).Streams);

        if (!Cap.StreamIdsCaseInsensitive)
            return;

        var otherCase = streamId.Replace(":Ab", ":aB", StringComparison.Ordinal);
        await SetStreamIdAsync(opened, streamId, otherCase);
        var rows = await ReadStreamTagsAsync(opened);
        Assert.Equal(["beta"], TagsFor(rows, otherCase));
        Assert.DoesNotContain(rows, row => row.StreamId == streamId);
    }

    [ContractFact]
    public async Task StreamTagTable_Backfill_RunsOnlyWhenTheTableIsCreated()
    {
        SkipUnless(Cap.HasStreamTagTable, "This store has no stream tag table.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var first = "acme:Order:" + Guid.NewGuid().ToString("N");
        var second = "acme:Order:" + Guid.NewGuid().ToString("N");
        await AppendAsync(opened.Store, first, "red", "blue");
        await AppendAsync(opened.Store, second, "red");

        await DropStreamTagTableAsync(opened);
        await InitializeAsync(opened);

        var rows = await ReadStreamTagsAsync(opened);
        Assert.Equal(["blue", "red"], TagsFor(rows, first));
        Assert.Equal(["red"], TagsFor(rows, second));
        Assert.Equal(2, (await opened.Store.ListStreamsAsync(new StreamListFilter { Tag = "red" })).Streams.Count);
        Assert.Equal(CurrentFormat(opened), await ReadFormatAsync(opened));

        await DeleteStreamTagsAsync(opened);
        await InitializeAsync(opened);
        Assert.Empty(await ReadStreamTagsAsync(opened));
    }

    private static async Task AppendAsync(IEventStore store, string streamId, params string[] tags)
    {
        var tenant = StreamIdParser.ExtractTenantId(streamId);
        await store.AppendAsync(
            streamId,
            [new TaggedEvent(ContractEvent.Tick(), tags)],
            metadata: tenant is null ? null : new EventMetadata { TenantId = tenant });
    }

    private static async Task<List<(string Tag, string StreamId)>> ReadStreamTagsAsync(ContractEventStore opened)
    {
        if (opened.Store is SqlServerEventStore sql)
        {
            var hooks = (SqlServerContractHooks)opened.Hooks;
            await using var connection = new SqlConnection(hooks.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand(
                $"SELECT Tag, StreamId FROM {sql.QualifiedStreamTagsTable}",
                connection);
            var rows = new List<(string, string)>();
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                rows.Add((reader.GetString(0), reader.GetString(1)));
            return rows;
        }

        var postgres = (PostgresEventStore)opened.Store;
        await using var pg = new NpgsqlConnection(postgres.ConnectionString);
        await pg.OpenAsync();
        await using var query = new NpgsqlCommand(
            $"SELECT tag, stream_id FROM {postgres.QualifiedStreamTagsTable}",
            pg);
        var found = new List<(string, string)>();
        await using var pgReader = await query.ExecuteReaderAsync();
        while (await pgReader.ReadAsync())
            found.Add((pgReader.GetString(0), pgReader.GetString(1)));
        return found;
    }

    private static async Task ReplaceTagsAsync(ContractEventStore opened, string streamId, params string[] tags)
    {
        if (opened.Store is SqlServerEventStore sql)
        {
            var hooks = (SqlServerContractHooks)opened.Hooks;
            var json = "[" + string.Join(",", tags.Select(tag => "\"" + tag + "\"")) + "]";
            await using var connection = new SqlConnection(hooks.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand(
                $"UPDATE {sql.QualifiedRegistryTable} SET Tags = @Tags WHERE StreamId = @StreamId",
                connection);
            command.Parameters.AddWithValue("@Tags", json);
            command.Parameters.AddWithValue("@StreamId", streamId);
            Assert.Equal(1, await command.ExecuteNonQueryAsync());
            return;
        }

        var postgres = (PostgresEventStore)opened.Store;
        await using var pg = new NpgsqlConnection(postgres.ConnectionString);
        await pg.OpenAsync();
        await using var query = new NpgsqlCommand(
            $"UPDATE {postgres.QualifiedStreamsTable} SET tags = @tags WHERE stream_id = @stream",
            pg);
        query.Parameters.Add(new NpgsqlParameter("tags", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text) { Value = tags });
        query.Parameters.AddWithValue("stream", streamId);
        Assert.Equal(1, await query.ExecuteNonQueryAsync());
    }

    private static async Task SetStreamIdAsync(ContractEventStore opened, string fromId, string toId)
    {
        var sql = (SqlServerEventStore)opened.Store;
        var hooks = (SqlServerContractHooks)opened.Hooks;
        await using var connection = new SqlConnection(hooks.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            $"UPDATE {sql.QualifiedRegistryTable} SET StreamId = @ToId WHERE StreamId = @FromId",
            connection);
        command.Parameters.AddWithValue("@ToId", toId);
        command.Parameters.AddWithValue("@FromId", fromId);
        Assert.Equal(1, await command.ExecuteNonQueryAsync());
    }

    private static async Task DropStreamTagTableAsync(ContractEventStore opened)
    {
        if (opened.Store is SqlServerEventStore sql)
        {
            var hooks = (SqlServerContractHooks)opened.Hooks;
            await using var connection = new SqlConnection(hooks.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand($"DROP TABLE {sql.QualifiedStreamTagsTable}", connection);
            await command.ExecuteNonQueryAsync();
            return;
        }

        var postgres = (PostgresEventStore)opened.Store;
        var schemaInfo = PgIdentifier.Qualify(postgres.SchemaName, "schema_info");
        var admin = ((PostgresContractHooks)opened.Hooks).AdminConnectionString;
        await using var pg = new NpgsqlConnection(admin);
        await pg.OpenAsync();
        await using var clear = new NpgsqlCommand(
            $"DELETE FROM {schemaInfo} WHERE component = 'stream_tags'",
            pg);
        await clear.ExecuteNonQueryAsync();
        await using var drop = new NpgsqlCommand($"DROP TABLE IF EXISTS {postgres.QualifiedStreamTagsTable} CASCADE", pg);
        await drop.ExecuteNonQueryAsync();
    }

    private static async Task InitializeAsync(ContractEventStore opened)
    {
        if (opened.Store is SqlServerEventStore sql)
            await sql.InitializeSchemaAsync();
        else if (opened.Store is PostgresEventStore postgres)
            await postgres.InitializeSchemaAsync();
    }

    private static async Task DeleteStreamTagsAsync(ContractEventStore opened)
    {
        if (opened.Store is SqlServerEventStore sql)
        {
            var hooks = (SqlServerContractHooks)opened.Hooks;
            await using var connection = new SqlConnection(hooks.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand($"DELETE FROM {sql.QualifiedStreamTagsTable}", connection);
            await command.ExecuteNonQueryAsync();
            return;
        }

        var postgres = (PostgresEventStore)opened.Store;
        await using var pg = new NpgsqlConnection(postgres.ConnectionString);
        await pg.OpenAsync();
        await using var query = new NpgsqlCommand($"DELETE FROM {postgres.QualifiedStreamTagsTable}", pg);
        await query.ExecuteNonQueryAsync();
    }

    private static async Task<string> ReadFormatAsync(ContractEventStore opened)
    {
        if (opened.Store is SqlServerEventStore sql)
        {
            var hooks = (SqlServerContractHooks)opened.Hooks;
            await using var connection = new SqlConnection(hooks.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand(
                """
                SELECT CAST(value AS nvarchar(128))
                FROM sys.extended_properties
                WHERE major_id = OBJECT_ID(@Table)
                  AND name = @Name
                  AND minor_id = 0
                """,
                connection);
            command.Parameters.AddWithValue("@Table", sql.QualifiedEventsTable);
            command.Parameters.AddWithValue("@Name", SqlServerEventStore.SchemaFormatPropertyName);
            return Convert.ToString(await command.ExecuteScalarAsync()) ?? "";
        }

        var postgres = (PostgresEventStore)opened.Store;
        var schemaInfo = PgIdentifier.Qualify(postgres.SchemaName, "schema_info");
        await using var pg = new NpgsqlConnection(postgres.ConnectionString);
        await pg.OpenAsync();
        await using var query = new NpgsqlCommand(
            $"SELECT format FROM {schemaInfo} WHERE component = 'events'",
            pg);
        return Convert.ToString(await query.ExecuteScalarAsync()) ?? "";
    }

    private static string CurrentFormat(ContractEventStore opened)
        => opened.Store is PostgresEventStore
            ? PostgresEventStore.CurrentSchemaFormat.ToString()
            : SqlServerEventStore.CurrentSchemaFormat;

    private static List<string> TagsFor(IEnumerable<(string Tag, string StreamId)> rows, string streamId)
        => rows.Where(row => row.StreamId == streamId).Select(row => row.Tag).OrderBy(tag => tag, StringComparer.Ordinal).ToList();
}
