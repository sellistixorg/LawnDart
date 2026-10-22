using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace LawnDart.Backends.Contract.Tests;

public abstract class StreamListContractTests : ContractSuite
{
    protected StreamListContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task ListStreams_OrdersByStreamId_CharacterByCharacter()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        // B, Z, _, a. A case-insensitive primary key would still keep these as four streams.
        // A culture sort puts "a" before "B". Character order puts "B" first and "_" before "a".
        foreach (var id in new[] { "acme:Order:a", "acme:Order:_", "acme:Order:Z", "acme:Order:B" })
            await AppendAsync(store, id);

        var page = await DrainAsync(store, new StreamListFilter { Prefix = "acme:Order:" }, pageSize: 2);
        Assert.Equal(["acme:Order:B", "acme:Order:Z", "acme:Order:_", "acme:Order:a"], page.Ids);
        Assert.Null(page.LastToken);
    }

    [Fact]
    public async Task ListStreams_WhileAppendsRun_DoesNotSkipOrRepeat()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var prefix = "acme:Order:";
        var initial = new List<string>();
        for (var i = 0; i < 20; i++)
        {
            var id = prefix + i.ToString("D4");
            initial.Add(id);
            await AppendAsync(store, id);
        }

        var appending = Task.Run(async () =>
        {
            for (var i = 0; i < 8; i++)
            {
                await AppendAsync(store, prefix + "m" + i.ToString("D4"));
                await AppendAsync(store, prefix + " " + i.ToString("D4"));
            }
        });

        var seen = new List<string>();
        string? token = null;
        var filter = new StreamListFilter { Prefix = prefix };
        do
        {
            var page = await store.ListStreamsAsync(filter, token, pageSize: 3);
            seen.AddRange(page.Streams.Select(stream => stream.StreamId));
            token = page.NextPageToken;
        }
        while (token is not null);

        await appending;

        Assert.Equal(seen.Count, seen.Distinct(StringComparer.Ordinal).Count());
        for (var i = 1; i < seen.Count; i++)
            Assert.True(string.CompareOrdinal(seen[i - 1], seen[i]) < 0);
        foreach (var id in initial)
            Assert.Contains(id, seen);
    }

    [Fact]
    public async Task ListStreams_FiltersCombine()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await AppendAsync(store, "acme:Order:1", "red");
        await AppendAsync(store, "acme:Order:2", "blue");
        await AppendAsync(store, "acme:Customer:1", "red");
        await AppendAsync(store, "other:Order:1", "red");

        var page = await store.ListStreamsAsync(new StreamListFilter
        {
            Prefix = "acme:",
            Tag = "red",
            AggregateType = "Order"
        });

        Assert.Equal(["acme:Order:1"], page.Streams.Select(stream => stream.StreamId).ToArray());
        Assert.Null(page.NextPageToken);
    }

    [Fact]
    public async Task ListStreams_Prefix_TreatsLikeMetacharactersAsLiteral()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var suffix = Guid.NewGuid().ToString("N");
        await AppendAsync(store, $"acme_west{suffix}:Order:1");
        await AppendAsync(store, $"acmeXwest{suffix}:Order:1");

        var page = await store.ListStreamsAsync(new StreamListFilter { Prefix = $"acme_west{suffix}:" });
        Assert.Equal([$"acme_west{suffix}:Order:1"], page.Streams.Select(stream => stream.StreamId).ToArray());
    }

    [Fact]
    public async Task ListStreams_DigitPrefix_IsTheSameSetOnEveryPageSize()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var matching = new[]
        {
            "7:Order:2",
            "7:Order:10",
            "7:Order:b",
            "7:Order:01",
            "7:Order:a",
            "7:Order:02",
            "7:Order:c",
            "7:Order:d",
        };
        var digitTag = new HashSet<string>(StringComparer.Ordinal)
        {
            "7:Order:01",
            "7:Order:02",
            "7:Order:2",
            "7:Order:b",
        };
        foreach (var id in matching)
            await AppendAsync(store, id, digitTag.Contains(id) ? ["digit"] : []);

        await AppendAsync(store, "70:Order:1", "digit");
        await AppendAsync(store, "8:Order:1");
        await AppendAsync(store, "a:Order:1", "digit");

        var expected = matching.OrderBy(id => id, StringComparer.Ordinal).ToArray();
        foreach (var pageSize in new[] { 1, 3, 100 })
        {
            var page = await DrainAsync(store, new StreamListFilter { Prefix = "7:" }, pageSize);
            Assert.Equal(expected, page.Ids);
            Assert.Null(page.LastToken);
            Assert.Equal(page.Ids.Count, page.Ids.Distinct(StringComparer.Ordinal).Count());
            Assert.DoesNotContain("70:Order:1", page.Ids);
            Assert.DoesNotContain("8:Order:1", page.Ids);
            Assert.DoesNotContain("a:Order:1", page.Ids);
        }

        var tagged = digitTag.OrderBy(id => id, StringComparer.Ordinal).ToArray();
        foreach (var pageSize in new[] { 1, 2, 50 })
        {
            var page = await DrainAsync(store, new StreamListFilter { Prefix = "7:", Tag = "digit" }, pageSize);
            Assert.Equal(tagged, page.Ids);
            Assert.Null(page.LastToken);
            Assert.DoesNotContain("70:Order:1", page.Ids);
            Assert.DoesNotContain("a:Order:1", page.Ids);
        }
    }

    [Fact]
    public async Task ListStreams_TagFilter_PagesEveryMatch()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var expected = new List<string>();
        for (var i = 0; i < 25; i++)
        {
            var id = "acme:Order:" + i.ToString("D4");
            expected.Add(id);
            await AppendAsync(store, id, "shared");
        }

        await AppendAsync(store, "acme:Order:other", "lonely");
        expected.Sort(StringComparer.Ordinal);

        var page = await DrainAsync(store, new StreamListFilter { Tag = "shared" }, pageSize: 7);
        Assert.Equal(expected, page.Ids);
        Assert.DoesNotContain("acme:Order:other", page.Ids);
    }

    [Fact]
    public async Task ListStreams_LaterAppend_AddsTag()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = "acme:Order:" + Guid.NewGuid().ToString("N");
        await AppendAsync(store, streamId, "alpha");
        await AppendAsync(store, streamId, "beta");

        var alpha = await store.ListStreamsAsync(new StreamListFilter { Tag = "alpha" });
        var beta = await store.ListStreamsAsync(new StreamListFilter { Tag = "beta" });
        Assert.Equal(streamId, Assert.Single(alpha.Streams).StreamId);
        Assert.Equal(streamId, Assert.Single(beta.Streams).StreamId);
        Assert.Contains("alpha", alpha.Streams[0].Tags);
        Assert.Contains("beta", beta.Streams[0].Tags);
    }

    [Fact]
    public async Task ListStreams_TokenRoundTrip_AndEmptyLastPage()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await AppendAsync(store, "acme:Order:1");
        await AppendAsync(store, "acme:Order:2");
        var filter = new StreamListFilter { Prefix = "acme:Order:" };

        var first = await store.ListStreamsAsync(filter, pageSize: 1);
        Assert.Equal("acme:Order:1", Assert.Single(first.Streams).StreamId);
        Assert.NotNull(first.NextPageToken);

        var second = await store.ListStreamsAsync(filter, first.NextPageToken, pageSize: 1);
        Assert.Equal("acme:Order:2", Assert.Single(second.Streams).StreamId);
        Assert.Null(second.NextPageToken);

        var pastEnd = StreamPageToken.Encode("acme:Order:zzz", filter);
        var empty = await store.ListStreamsAsync(filter, pastEnd, pageSize: 1);
        Assert.Empty(empty.Streams);
        Assert.Null(empty.NextPageToken);
    }

    [Fact]
    public async Task ListStreams_NoMatches_ReturnsEmptyPage()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await AppendAsync(store, "acme:Order:1", "red");
        var page = await store.ListStreamsAsync(new StreamListFilter { Tag = "missing" });
        Assert.Empty(page.Streams);
        Assert.Null(page.NextPageToken);
    }

    [Fact]
    public async Task ListStreams_BadToken_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await Assert.ThrowsAsync<ArgumentException>(() => store.ListStreamsAsync(pageToken: "@@@"));
        await Assert.ThrowsAsync<ArgumentException>(() => store.ListStreamsAsync(pageToken: "YQ"));
    }

    [Fact]
    public async Task ListStreams_TokenFromAnotherFilter_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await AppendAsync(store, "acme:Order:1", "red");
        await AppendAsync(store, "acme:Order:2", "red");
        var issued = await store.ListStreamsAsync(new StreamListFilter { Tag = "red" }, pageSize: 1);
        Assert.NotNull(issued.NextPageToken);
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.ListStreamsAsync(new StreamListFilter { Tag = "blue" }, issued.NextPageToken));
    }

    [Fact]
    public async Task ListStreams_PageSize_IsBounded()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.ListStreamsAsync(pageSize: 0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.ListStreamsAsync(pageSize: -1));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.ListStreamsAsync(pageSize: StreamPage.MaxPageSize + 1));

        var widest = await store.ListStreamsAsync(pageSize: StreamPage.MaxPageSize);
        Assert.Empty(widest.Streams);
        Assert.Null(widest.NextPageToken);

        await AppendAsync(store, "acme:Order:1");
        var one = await store.ListStreamsAsync(pageSize: 1);
        Assert.Single(one.Streams);
        Assert.Null(one.NextPageToken);
    }

    [Fact]
    public async Task ListStreams_ArchivedStream_IsOmitted()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var store = opened.Store;
        var streamId = "acme:Order:" + Guid.NewGuid().ToString("N");
        await AppendAsync(store, streamId, "red");
        await MarkArchivedAsync(opened, streamId);

        Assert.NotNull(await store.GetStreamAsync(streamId));
        Assert.Equal(StreamStatus.Archived, (await store.GetStreamAsync(streamId))!.Status);
        Assert.Empty((await store.ListStreamsAsync(new StreamListFilter { Prefix = "acme:Order:" })).Streams);
        Assert.Empty(await store.EnumerateStreamIdsAsync("acme:Order:"));
        Assert.Equal(0, await store.GetStreamCountAsync("acme:Order:"));
        Assert.Empty(await store.GetStreamsByTagAsync("red"));
        Assert.Empty(await store.GetStreamsByAggregateTypeAsync("Order"));
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
            [ContractEvent.Tick()],
            metadata: tenant is null ? null : new EventMetadata { TenantId = tenant },
            tags: tags);
    }

    private static async Task<(List<string> Ids, string? LastToken)> DrainAsync(
        IEventStore store,
        StreamListFilter filter,
        int pageSize)
    {
        var ids = new List<string>();
        string? token = null;
        string? last = null;
        do
        {
            var page = await store.ListStreamsAsync(filter, token, pageSize);
            ids.AddRange(page.Streams.Select(stream => stream.StreamId));
            last = page.NextPageToken;
            token = page.NextPageToken;
        }
        while (token is not null);

        return (ids, last);
    }

    private static async Task MarkArchivedAsync(ContractEventStore opened, string streamId)
    {
        if (opened.Store is SqlServerEventStore sql)
        {
            var hooks = (SqlServerContractHooks)opened.Hooks;
            await using var connection = new SqlConnection(hooks.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand(
                $"UPDATE {sql.QualifiedRegistryTable} SET Status = N'Archived' WHERE StreamId = @StreamId",
                connection);
            command.Parameters.AddWithValue("@StreamId", streamId);
            Assert.Equal(1, await command.ExecuteNonQueryAsync());
            return;
        }

        if (opened.Store is PostgresEventStore postgres)
        {
            await using var connection = new NpgsqlConnection(postgres.ConnectionString);
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(
                $"UPDATE {postgres.QualifiedStreamsTable} SET status = 'Archived' WHERE stream_id = @stream",
                connection);
            command.Parameters.AddWithValue("stream", streamId);
            Assert.Equal(1, await command.ExecuteNonQueryAsync());
            return;
        }

        var metadata = await opened.Store.GetStreamAsync(streamId);
        Assert.NotNull(metadata);
        metadata!.Status = StreamStatus.Archived;
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
