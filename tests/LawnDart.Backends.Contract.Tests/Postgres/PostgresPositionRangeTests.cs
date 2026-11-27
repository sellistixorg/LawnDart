using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Npgsql;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Events and event_tags split into position ranges: ranges made ahead of time,
/// appends across range edges, ranges off for Isolated, and the schema_info stamp.
/// </summary>
[Collection("PostgresContract")]
[Trait("Category", "Integration")]
public sealed class PostgresPositionRangeTests
{
    private const long Small = 10;

    private readonly PostgresContractFixture _fixture;

    public PostgresPositionRangeTests(PostgresContractFixture fixture) => _fixture = fixture;

    [Theory]
    [InlineData(Tenancy.Shared)]
    [InlineData(Tenancy.Single)]
    public async Task Init_MakesTwoEmptyRangesAhead_AndStampsTheSize(Tenancy tenancy)
    {
        var opened = await OpenAsync(tenancy, Small);
        await using var connection = await ConnectAsync();

        Assert.Equal(["events_p0", "events_p1", "events_p2"], await RangesAsync(connection, opened.SchemaName, "events"));
        Assert.Equal(["event_tags_p0", "event_tags_p1", "event_tags_p2"], await RangesAsync(connection, opened.SchemaName, "event_tags"));
        Assert.Equal("p", await RelKindAsync(connection, opened.SchemaName, "events"));
        Assert.Equal("p", await RelKindAsync(connection, opened.SchemaName, "event_tags"));
        Assert.Contains("parallel_workers=0", await RelOptionsAsync(connection, opened.SchemaName, "event_tags_p2"));
        Assert.DoesNotContain("parallel_workers=0", await RelOptionsAsync(connection, opened.SchemaName, "events_p2"));

        var stamp = await StampAsync(connection, opened.SchemaName);
        Assert.Equal("7", stamp.Format);
        Assert.True(stamp.Partitioning);
        Assert.Equal(Small, stamp.Size);

        var check = await Store(opened).VerifySchemaAsync();
        Assert.True(check.Matches, string.Join("; ", check.Missing));
    }

    [Fact]
    public async Task Default_StampsTheDefaultSize()
    {
        var opened = await OpenAsync(Tenancy.Shared, rangeSize: null);
        await using var connection = await ConnectAsync();
        var stamp = await StampAsync(connection, opened.SchemaName);
        Assert.True(stamp.Partitioning);
        Assert.Equal(PostgresContractBackend.TestPartitionSize, stamp.Size);
        Assert.Equal(3, (await RangesAsync(connection, opened.SchemaName, "events")).Count);
    }

    [Theory]
    [InlineData(Tenancy.Single, null, true)]
    [InlineData(Tenancy.Shared, null, true)]
    [InlineData(Tenancy.Isolated, null, false)]
    [InlineData(Tenancy.Shared, false, false)]
    [InlineData(Tenancy.Isolated, true, true)]
    public void RangesAreOnUnlessIsolated(Tenancy tenancy, bool? setting, bool expected)
        => Assert.Equal(
            expected,
            new PostgresEventStoreOptions { Tenancy = tenancy, PositionPartitioning = setting }.UsesPositionPartitions);

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    [InlineData(9_999)]
    [InlineData(1_000_000_000_000_001)]
    public void PartitionSize_OutsideTheBounds_IsRefused(long size)
    {
        var options = new PostgresEventStoreOptions { ConnectionString = "Host=x", PartitionSize = size };
        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.EnsureValid);
        Assert.Equal(nameof(PostgresEventStoreOptions.PartitionSize), ex.ParamName);
    }

    [Theory]
    [InlineData(10_000)]
    [InlineData(1_000_000_000_000_000)]
    public void PartitionSize_AtTheBounds_IsAllowed(long size)
        => new PostgresEventStoreOptions { ConnectionString = "Host=x", PartitionSize = size }.EnsureValid();

    [Fact]
    public void PartitionSize_TinyIsOnlyForTests()
        => new PostgresEventStoreOptions { ConnectionString = "Host=x", PartitionSize = 3, AllowTinyPartitionsForTests = true }.EnsureValid();

    [Fact]
    public async Task OldFormat1Schema_IsRefusedCleanly()
    {
        var schema = "old1_" + Guid.NewGuid().ToString("N")[..8];
        await using (var admin = new NpgsqlConnection(_fixture.AdminConnectionString))
        {
            await admin.OpenAsync();
            // The stamp table as format 1 wrote it: five columns, no partition_size.
            await ExecAsync(admin, $"""
                CREATE SCHEMA {schema};
                CREATE TABLE {schema}.schema_info (
                    component text PRIMARY KEY,
                    format int NOT NULL,
                    partition_count int NOT NULL,
                    partitioning boolean NOT NULL,
                    updated_at timestamptz NOT NULL);
                INSERT INTO {schema}.schema_info VALUES ('events', 1, 8, true, now());
                CREATE TABLE {schema}.events (global_position bigint, tenant_id text);
                """);
        }

        var store = new PostgresEventStore(
            _fixture.AdminConnectionString,
            options: new PostgresEventStoreOptions { ConnectionString = _fixture.AdminConnectionString, SchemaName = schema });
        var ex = await Assert.ThrowsAsync<IncompatibleEventStoreSchemaException>(() => store.InitializeSchemaAsync());
        Assert.Contains(schema, ex.Message, StringComparison.Ordinal);

        var check = await store.VerifySchemaAsync();
        Assert.False(check.Matches);
        Assert.Equal("1", check.FoundFormat);
        Assert.DoesNotContain(check.Missing, m => m.Contains("position ranges", StringComparison.Ordinal));

        await using var verify = new NpgsqlConnection(_fixture.AdminConnectionString);
        await verify.OpenAsync();
        Assert.Equal(0, await CountAsync(verify, $"SELECT count(*) FROM pg_attribute WHERE attrelid = '{schema}.schema_info'::regclass AND attname = 'partition_size'"));
    }

    [Fact]
    public async Task HandProvisionedRole_WithTheDocumentedGrants_AppendsPastTheSetupRanges()
    {
        var opened = await OpenAsync(Tenancy.Shared, Small);
        var schema = Quote(opened.SchemaName);
        var role = "pr6_hand_" + Guid.NewGuid().ToString("N")[..8];
        await using (var admin = new NpgsqlConnection(_fixture.AdminConnectionString))
        {
            await admin.OpenAsync();
            // Only the grants POSTGRES_PARTITIONS.md lists for an app role made by hand.
            await ExecAsync(admin, $"""
                CREATE ROLE {role} LOGIN PASSWORD '{role}' NOSUPERUSER NOCREATEDB NOCREATEROLE;
                GRANT CONNECT ON DATABASE lawndart TO {role};
                GRANT USAGE ON SCHEMA {schema} TO {role};
                GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {role};
                GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {schema} TO {role};
                GRANT EXECUTE ON FUNCTION {schema}.append_to_stream TO {role};
                SELECT setval('{opened.SchemaName}.event_position', {20 * Small});
                """);
        }

        var connection = new NpgsqlConnectionStringBuilder(_fixture.AdminConnectionString) { Username = role, Password = role }.ConnectionString;
        var store = new PostgresEventStore(
            connection,
            options: new PostgresEventStoreOptions
            {
                ConnectionString = connection,
                SchemaName = opened.SchemaName,
                ContextName = opened.SchemaName,
                Tenancy = Tenancy.Shared,
                PartitionSize = Small,
                AllowTinyPartitionsForTests = true
            });
        var stream = StreamId.For("acme", "Order", Guid.NewGuid().ToString("N"));
        var saved = await store.AppendAsync(stream, [new TaggedEvent(ContractEvent.Tick(), [Tag.Parse("hand:x")])]);
        Assert.True(saved.SequencePositions[0] > 20 * Small);
        await store.WaitForRangesAheadAsync();
        Assert.Single(await store.ReadStreamAsync(stream));
        Assert.True((await store.VerifySchemaAsync()).Matches);
    }

    [Fact]
    public async Task EnsurePartitions_InRepeatableRead_CountsARangeMadeMeanwhileAsDone()
    {
        var opened = await OpenAsync(Tenancy.Shared, Small);
        await using var reader = await ConnectAsync();
        await using var transaction = await reader.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead);
        // Take the snapshot before the other caller makes the ranges.
        await using (var snapshot = new NpgsqlCommand("SELECT count(*) FROM pg_catalog.pg_inherits", reader, transaction))
            await snapshot.ExecuteScalarAsync();

        await using (var other = await ConnectAsync())
            await ExecAsync(other, $"SELECT setval('{opened.SchemaName}.event_position', {(5 * Small) + 1})");
        await Store(opened).EnsurePartitionsAsync();

        await using (var again = new NpgsqlCommand($"SELECT {Quote(opened.SchemaName)}.ensure_event_partitions(NULL, 2)", reader, transaction))
            Assert.Equal((8 * Small) + 1, (long)(await again.ExecuteScalarAsync())!);
        await transaction.CommitAsync();

        await using var check = await ConnectAsync();
        Assert.Equal(8, (await RangesAsync(check, opened.SchemaName, "events")).Count);
        Assert.Equal(8, (await RangesAsync(check, opened.SchemaName, "event_tags")).Count);
    }

    [Fact]
    public async Task Writers_AndAConditionalAppend_AcrossTinyRanges_AllSave()
    {
        // Many writers and a conditional append race over tiny ranges, so ranges are
        // attached while appends run. Every append must save. The retry on "no partition"
        // is defensive: this race has not been seen to reach it, and the error filter has
        // its own unit test (PostgresRangeRetryFilterTests).
        for (var rep = 0; rep < 4; rep++)
        {
            var opened = await OpenAsync(Tenancy.Single, 7);
            var log = opened.Log;
            for (var round = 0; round < 10; round++)
            {
                var type = "race." + Guid.NewGuid().ToString("N");
                var after = await log.GetCurrentSequenceAsync();
                var query = Query.FromItems(QueryItem.ByType(type));
                var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var ready = 0;
                var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(i => Task.Run(async () =>
                {
                    if (Interlocked.Increment(ref ready) == 16)
                        gate.SetResult();
                    await gate.Task;
                    try
                    {
                        if (i == 8)
                            await log.AppendAsync([RaceEvent("race.checker." + Guid.NewGuid().ToString("N"))], AppendCondition.FailIfMatches(query, after));
                        else
                            await log.AppendAsync(StreamId.For("race", Guid.NewGuid().ToString("N")), [RaceEvent(type)]);
                        return (Exception?)null;
                    }
                    catch (Exception ex)
                    {
                        return ex;
                    }
                })));
                Assert.All(results, e => Assert.True(e is null or ConcurrencyException, e?.ToString()));
                Assert.Equal(15, (await log.ReadByQueryAsync(query, fromSequencePosition: after + 1)).Events.Count);
            }
        }
    }

    private static AppendEvent RaceEvent(string type) => new(type, System.Text.Encoding.UTF8.GetBytes("{}"));

    [Fact]
    public async Task BackgroundFailure_PausesAndWarnsOnce()
    {
        var opened = await OpenAsync(Tenancy.Shared, Small);
        var store = Store(opened);
        await using (var admin = new NpgsqlConnection(_fixture.AdminConnectionString))
        {
            await admin.OpenAsync();
            await ExecAsync(admin, $"REVOKE EXECUTE ON FUNCTION {Quote(opened.SchemaName)}.ensure_event_partitions(bigint, int) FROM PUBLIC, lawndart_app");
        }

        // Positions 1 to 30 have ranges, so these appends never need the function
        // themselves, but each one past position 10 wants a range made ahead.
        var stream = StreamId.For("acme", "Order", Guid.NewGuid().ToString("N"));
        for (var i = 0; i < 25; i++)
        {
            await opened.Store.AppendAsync(stream, [ContractEvent.Tick()]);
            await store.WaitForRangesAheadAsync();
        }

        Assert.True(store.RangesAheadPaused);
        Assert.Single(opened.ReadLogEntries(), e => e.Message.Contains("position ranges ahead", StringComparison.Ordinal));
        Assert.Equal(25, (await opened.Store.ReadStreamAsync(stream)).Count);
    }

    [Fact]
    public async Task KnownPositionAboveTheSequence_PausesInsteadOfLooping()
    {
        var opened = await OpenAsync(Tenancy.Shared, Small);
        var store = Store(opened);
        var stream = StreamId.For("acme", "Order", Guid.NewGuid().ToString("N"));
        await opened.Store.AppendAsync(stream, [ContractEvent.Tick()]);
        await store.WaitForRangesAheadAsync();

        // A position far above what the sequence handed out, as after a restore
        // or a sequence reset. The range function cannot go past the sequence.
        store.PublishSignal(Small * 50);
        await opened.Store.AppendAsync(stream, [ContractEvent.Tick()]);
        await Task.Delay(500);
        await store.WaitForRangesAheadAsync();
        var calls = store.RangesAheadCalls;
        await Task.Delay(1000);
        for (var i = 0; i < 5; i++)
            await opened.Store.AppendAsync(stream, [ContractEvent.Tick()]);
        await store.WaitForRangesAheadAsync();

        Assert.True(store.RangesAheadPaused);
        Assert.Equal(calls, store.RangesAheadCalls);
        Assert.True(calls < 5, $"range function calls: {calls}");
        Assert.Equal(7, (await opened.Store.ReadStreamAsync(stream)).Count);
    }

    [Fact]
    public async Task RangesOff_OnShared_IsAPlainTable()
    {
        var opened = await OpenAsync(Tenancy.Shared, Small, options => options.PositionPartitioning = false);
        await using var connection = await ConnectAsync();
        Assert.Equal("r", await RelKindAsync(connection, opened.SchemaName, "events"));
        Assert.Equal("r", await RelKindAsync(connection, opened.SchemaName, "event_tags"));
        Assert.Contains("parallel_workers=0", await RelOptionsAsync(connection, opened.SchemaName, "event_tags"));
        Assert.Empty(await RangesAsync(connection, opened.SchemaName, "events"));
        var stamp = await StampAsync(connection, opened.SchemaName);
        Assert.False(stamp.Partitioning);
        Assert.Null(stamp.Size);

        var stream = StreamId.For("acme", "Order", Guid.NewGuid().ToString("N"));
        await opened.Store.AppendAsync(stream, Enumerable.Range(0, 25).Select(_ => new TaggedEvent(ContractEvent.Tick(), [])).ToArray());
        Assert.Equal(25, (await opened.Store.ReadStreamAsync(stream)).Count);
        Assert.Equal(0, Store(opened).KnownPartitionLimit);
        Assert.Empty(await RangesAsync(connection, opened.SchemaName, "events"));
        var check = await Store(opened).VerifySchemaAsync();
        Assert.True(check.Matches, string.Join("; ", check.Missing));
    }

    [Fact]
    public async Task Appends_CrossRangeEdges_AndEveryReadSeesThem()
    {
        var opened = await OpenAsync(Tenancy.Shared, Small);
        var store = Store(opened);
        var tag = Tag.Parse("cross:" + Guid.NewGuid().ToString("N"));
        var stream = StreamId.For("acme", "Order", Guid.NewGuid().ToString("N"));

        // Single events, then one batch that spans more than two ranges at once.
        var positions = new List<long>();
        for (var i = 0; i < 7; i++)
            positions.AddRange((await opened.Store.AppendAsync(stream, [new TaggedEvent(ContractEvent.Tick(), [tag])])).SequencePositions);
        var batch = Enumerable.Range(0, 35).Select(_ => new TaggedEvent(ContractEvent.Tick(), [tag])).ToArray();
        positions.AddRange((await opened.Store.AppendAsync(stream, batch)).SequencePositions);
        positions.AddRange((await DcbAsync(opened.Store, [new TaggedEvent(ContractEvent.Tick(), [tag])], tag, positions[^1])).SequencePositions);
        await store.WaitForRangesAheadAsync();

        Assert.Equal(43, positions.Count);
        Assert.True(positions[^1] > 4 * Small);

        var byStream = await opened.Store.ReadStreamAsync(stream);
        Assert.Equal(positions.Take(42), byStream.Select(e => e.SequencePosition));
        var fromVersion = await opened.Store.ReadStreamAsync(stream, fromVersion: 30);
        // Versions on IEventStore count from 1, so version 30 is the 30th event.
        Assert.Equal(positions.Skip(29).Take(13), fromVersion.Select(e => e.SequencePosition));

        var byTag = await opened.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tag)).ForTenant("acme"));
        Assert.Equal(positions, byTag.Events.Select(e => e.SequencePosition));
        var window = await opened.Store.ReadByQueryAsync(Query.All().ForTenant("acme"), fromSequencePosition: positions[5], toSequencePosition: positions[40]);
        Assert.Equal(positions.Skip(5).Take(36), window.Events.Select(e => e.SequencePosition));

        await using var connection = await ConnectAsync();
        var head = (positions[^1] - 1) / Small;
        var ranges = await RangesAsync(connection, opened.SchemaName, "events");
        Assert.Contains($"events_p{head + 2}", ranges);
        Assert.Equal(ranges.Count, (await RangesAsync(connection, opened.SchemaName, "event_tags")).Count);
        Assert.True(store.KnownPartitionLimit >= ((head + 3) * Small) + 1);
        Assert.Equal(0, await CountAsync(connection, $"SELECT count(*) FROM ONLY {Quote(opened.SchemaName)}.events"));
    }

    [Fact]
    public async Task Append_PastWhatThisStoreKnows_StillSaves()
    {
        var opened = await OpenAsync(Tenancy.Shared, Small);
        var store = Store(opened);
        await opened.Store.AppendAsync(StreamId.For("acme", "Order", Guid.NewGuid().ToString("N")), [ContractEvent.Tick()]);
        await store.WaitForRangesAheadAsync();
        var known = store.KnownPartitionLimit;
        Assert.True(known > 0);

        // Another writer moved far ahead: no range exists there and this store does not know it.
        await using (var connection = await ConnectAsync())
            await ExecAsync(connection, $"SELECT setval('{opened.SchemaName}.event_position', {known + (20 * Small)})");

        var stream = StreamId.For("acme", "Order", Guid.NewGuid().ToString("N"));
        var saved = await opened.Store.AppendAsync(stream, [ContractEvent.Tick(), ContractEvent.Tick()]);
        Assert.True(saved.SequencePositions[0] > known + (20 * Small));
        Assert.Equal(2, (await opened.Store.ReadStreamAsync(stream)).Count);

        var dcb = await DcbAsync(opened.Store, [new TaggedEvent(ContractEvent.Tick(), [Tag.Parse("past:x")])], Tag.Parse("past:x"), after: null);
        Assert.Single(dcb.SequencePositions);
    }

    [Fact]
    public async Task ManyWriters_OnTwoNodes_AcrossTinyRanges_AllSave()
    {
        const long tiny = 3;
        var shared = ContractStores.Isolated(tenancy: Tenancy.Shared);
        var backend = (PostgresContractBackend)_fixture.Backend;
        var first = await backend.OpenEventStoreAsync(shared, options => options.PartitionSize = tiny);
        var second = await backend.OpenEventStoreAsync(shared, options => options.PartitionSize = tiny);
        var nodes = new[] { first, second };

        var tasks = Enumerable.Range(0, 16).Select(async writer =>
        {
            var node = nodes[writer % 2];
            var stream = StreamId.For("acme", "Order", Guid.NewGuid().ToString("N"));
            var tag = Tag.Parse("writer:" + writer.ToString(System.Globalization.CultureInfo.InvariantCulture));
            var saved = new List<long>();
            for (var i = 0; i < 12; i++)
            {
                var result = writer % 4 == 3
                    ? await DcbAsync(node.Store, [new TaggedEvent(ContractEvent.Tick(), [tag]), new TaggedEvent(ContractEvent.Tick(), [tag])], Tag.Parse("none:" + Guid.NewGuid().ToString("N")), after: null)
                    : await node.Store.AppendAsync(stream, [new TaggedEvent(ContractEvent.Tick(), [tag])]);
                saved.AddRange(result.SequencePositions);
            }
            return saved;
        }).ToArray();
        var all = (await Task.WhenAll(tasks)).SelectMany(p => p).ToList();
        await Task.WhenAll(nodes.Select(n => Store(n).WaitForRangesAheadAsync()));

        Assert.Equal(all.Count, all.Distinct().Count());
        var read = await first.Store.ReadByQueryAsync(Query.All().ForTenant("acme"));
        Assert.Equal(all.Order(), read.Events.Select(e => e.SequencePosition));

        await using var connection = await ConnectAsync();
        var head = (all.Max() - 1) / tiny;
        var ranges = await RangesAsync(connection, first.SchemaName, "events");
        Assert.Contains($"events_p{head}", ranges);
        // Two empty ranges are still above the newest position once the writers stop.
        Assert.Contains($"events_p{head + 1}", ranges);
        Assert.Contains($"events_p{head + 2}", ranges);
        Assert.Equal(ranges, (await RangesAsync(connection, first.SchemaName, "event_tags")).Select(n => n.Replace("event_tags", "events", StringComparison.Ordinal)));
        Assert.Equal(0, await CountAsync(connection, $"SELECT count(*) FROM {Quote(first.SchemaName)}.events WHERE global_position > {(head + 1) * tiny}"));
        Assert.True((await Store(first).VerifySchemaAsync()).Matches);
    }

    [Fact]
    public async Task EnsurePartitions_FromTheAppRole_AddsNothingTwice()
    {
        var opened = await OpenAsync(Tenancy.Shared, Small);
        var store = Store(opened);
        await store.EnsurePartitionsAsync();
        await store.EnsurePartitionsAsync();
        await using var connection = await ConnectAsync();
        Assert.Equal(3, (await RangesAsync(connection, opened.SchemaName, "events")).Count);

        await ExecAsync(connection, $"SELECT setval('{opened.SchemaName}.event_position', {(5 * Small) + 1})");
        await store.EnsurePartitionsAsync();
        // Range 5 holds the newest position; ranges 3 and 4 are filled in, 6 and 7 are ahead.
        Assert.Equal(
            Enumerable.Range(0, 8).Select(k => $"events_p{k}"),
            await RangesAsync(connection, opened.SchemaName, "events"));
    }

    [Fact]
    public async Task Verify_ReportsAMissingHeadRange()
    {
        var opened = await OpenAsync(Tenancy.Shared, Small);
        await using (var admin = new NpgsqlConnection(_fixture.AdminConnectionString))
        {
            await admin.OpenAsync();
            await ExecAsync(admin, $"DROP TABLE {Quote(opened.SchemaName)}.event_tags_p0");
        }

        var check = await Store(opened).VerifySchemaAsync();
        Assert.False(check.Matches);
        Assert.Contains(check.Missing, m => m.Contains("event_tags", StringComparison.Ordinal));
    }

    private Task<ContractEventStore> OpenAsync(Tenancy tenancy, long? rangeSize, Action<PostgresEventStoreOptions>? configure = null)
        => ((PostgresContractBackend)_fixture.Backend).OpenEventStoreAsync(
            ContractStores.Isolated(tenancy: tenancy),
            options =>
            {
                if (rangeSize is { } size)
                    options.PartitionSize = size;
                configure?.Invoke(options);
            });

    private static Task<AppendResult> DcbAsync(IEventStore store, TaggedEvent[] events, Tag guard, long? after)
        => store.AppendAsync(
            events,
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(guard)).ForTenant("acme"), after),
            metadata: new EventMetadata { TenantId = "acme" });

    private static PostgresEventStore Store(ContractEventStore opened) => (PostgresEventStore)opened.Log;

    private async Task<NpgsqlConnection> ConnectAsync()
    {
        var connection = new NpgsqlConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    private static async Task<List<string>> RangesAsync(NpgsqlConnection connection, string schema, string parent)
    {
        await using var command = new NpgsqlCommand(
            """
            SELECT c.relname
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            JOIN pg_namespace n ON n.oid = p.relnamespace
            WHERE n.nspname = @schema AND p.relname = @parent
            ORDER BY length(c.relname), c.relname
            """,
            connection);
        command.Parameters.AddWithValue("schema", schema);
        command.Parameters.AddWithValue("parent", parent);
        var names = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            names.Add(reader.GetString(0));
        return names;
    }

    private static async Task<string> RelKindAsync(NpgsqlConnection connection, string schema, string table)
    {
        await using var command = new NpgsqlCommand(
            "SELECT c.relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = @schema AND c.relname = @table",
            connection);
        command.Parameters.AddWithValue("schema", schema);
        command.Parameters.AddWithValue("table", table);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<string[]> RelOptionsAsync(NpgsqlConnection connection, string schema, string table)
    {
        await using var command = new NpgsqlCommand(
            "SELECT coalesce(c.reloptions, '{}') FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = @schema AND c.relname = @table",
            connection);
        command.Parameters.AddWithValue("schema", schema);
        command.Parameters.AddWithValue("table", table);
        return (string[])(await command.ExecuteScalarAsync())!;
    }

    private static async Task<(string Format, bool Partitioning, long? Size)> StampAsync(NpgsqlConnection connection, string schema)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT format, partitioning, partition_size FROM {Quote(schema)}.schema_info WHERE component = 'events'",
            connection);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return (
            Convert.ToString(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture)!,
            reader.GetBoolean(1),
            reader.IsDBNull(2) ? null : reader.GetInt64(2));
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
