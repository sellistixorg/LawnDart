using System.Text;
using LawnDart;
using LawnDart.EventSourcing.Postgres;
using Microsoft.Extensions.Logging;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventStore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace LawnDart.Backends.Contract.Tests;

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
public sealed class PostgresPhase1Tests
{
    private readonly PostgresContractFixture _fixture;

    public PostgresPhase1Tests(PostgresContractFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Schema_HasHashBuckets_AndUniqueIndexesIncludeTenant()
    {
        var opened = await OpenAsync();
        await opened.Store.AppendAsync(Stream("acme"), [ContractEvent.Tick()]);
        await using var connection = await ConnectAsync();
        var tables = await NamesAsync(connection, opened.SchemaName, "events%");
        Assert.Contains("events_shared", tables);
        for (var i = 0; i < 8; i++)
            Assert.Contains("events_shared_h" + i, tables);

        await using var indexes = new NpgsqlCommand(
            """
            SELECT pg_get_indexdef(i.indexrelid)
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = @schema AND c.relname = 'events' AND i.indisunique
            """,
            connection);
        indexes.Parameters.AddWithValue("schema", opened.SchemaName);
        await using var reader = await indexes.ExecuteReaderAsync();
        var defs = new List<string>();
        while (await reader.ReadAsync())
            defs.Add(reader.GetString(0));
        Assert.NotEmpty(defs);
        Assert.All(defs, def => Assert.Contains("tenant_id", def, StringComparison.Ordinal));
    }

    [Fact]
    public async Task InitializeSchema_Twice_Succeeds_AndUnknownFormatThrows()
    {
        var opened = await OpenAsync();
        var store = (PostgresEventStore)opened.Store;
        await store.InitializeSchemaAsync();
        await store.InitializeSchemaAsync();

        await using var connection = await ConnectAsync();
        await using (var update = new NpgsqlCommand(
            $"UPDATE {Quote(opened.SchemaName)}.schema_info SET format = 99 WHERE component = 'events'",
            connection))
        {
            await update.ExecuteNonQueryAsync();
        }

        var ex = await Assert.ThrowsAsync<IncompatibleEventStoreSchemaException>(() => store.InitializeSchemaAsync());
        Assert.Contains("Drop and recreate", ex.Message, StringComparison.Ordinal);
        Assert.Equal(99, ex.FoundFormat);
    }

    [Fact]
    public async Task PartitionCount_Change_IsRefused()
    {
        var opened = await OpenAsync();
        var again = new PostgresEventStore(
            _fixture.ConnectionString,
            options: new PostgresEventStoreOptions
            {
                ConnectionString = _fixture.ConnectionString,
                SchemaName = opened.SchemaName,
                PartitionCount = 4
            });
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => again.InitializeSchemaAsync());
        Assert.Contains("hash buckets", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MissingTenant_StoresDefault_AndPrefixStoresThePrefix()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var bare = "Order:" + Guid.NewGuid().ToString("N");
        var prefixed = "acme:Order:" + Guid.NewGuid().ToString("N");
        var emptyPrefix = ":order:" + Guid.NewGuid().ToString("N");
        await opened.Store.AppendAsync(bare, [ContractEvent.Tick()]);
        await opened.Store.AppendAsync(prefixed, [ContractEvent.Tick()]);
        await opened.Store.AppendAsync(emptyPrefix, [ContractEvent.Tick()]);

        Assert.Equal("default", await TenantOfAsync(opened.SchemaName, bare));
        Assert.Equal("acme", await TenantOfAsync(opened.SchemaName, prefixed));
        Assert.Equal("default", await TenantOfAsync(opened.SchemaName, emptyPrefix));

        var bareMeta = await opened.Store.GetStreamAsync(bare);
        Assert.Null(bareMeta!.TenantId);
    }

    [Fact]
    public async Task EmptyTenant_IsRejectedByTheDatabase()
    {
        var opened = await OpenAsync();
        await using var connection = await ConnectAsync();
        var sql = $"""
            INSERT INTO {Quote(opened.SchemaName)}.event_types (token) VALUES ('empty-tenant-probe') ON CONFLICT DO NOTHING;
            INSERT INTO {Quote(opened.SchemaName)}.events (
                tenant_id, stream_id, version, global_position, event_id, event_type_id,
                schema_version, codec_id, data, partition_hash, recorded_at)
            SELECT '', 'x', 1, 1, gen_random_uuid(), id, 1, 1, to_jsonb(1), 1, now()
            FROM {Quote(opened.SchemaName)}.event_types WHERE token = 'empty-tenant-probe'
            """;
        var ex = await Assert.ThrowsAsync<PostgresException>(async () =>
        {
            await using var command = new NpgsqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        });
        Assert.Equal("23514", ex.SqlState);
    }

    [Fact]
    public async Task StreamRead_PlanTouchesOneHashBucket_AndGlobalReadIsOrdered()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var streams = new[]
        {
            "aa:Order:" + Guid.NewGuid().ToString("N"),
            "bb:Order:" + Guid.NewGuid().ToString("N"),
            "cc:Order:" + Guid.NewGuid().ToString("N")
        };
        foreach (var stream in streams)
            await opened.Store.AppendAsync(stream, [ContractEvent.Tick()]);

        await using var connection = await ConnectAsync();
        var explain = $"""
            EXPLAIN (FORMAT TEXT)
            SELECT version FROM {Quote(opened.SchemaName)}.events
            WHERE tenant_id = 'aa' AND stream_id = @stream AND version >= 0
            """;
        await using var command = new NpgsqlCommand(explain, connection);
        command.Parameters.AddWithValue("stream", streams[0]);
        var plan = new StringBuilder();
        await using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                plan.AppendLine(reader.GetString(0));
        }

        var hits = 0;
        for (var i = 0; i < 8; i++)
        {
            if (plan.ToString().Contains("events_shared_h" + i, StringComparison.Ordinal))
                hits++;
        }

        Assert.Equal(1, hits);

        var read = await opened.Log.ReadByQueryAsync(Query.All());
        var positions = read.Events.Select(e => e.SequencePosition).ToList();
        Assert.Equal(positions.Order(), positions);
    }

    [Fact]
    public async Task LosingAppend_DoesNotTakeASequenceNumber()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var stream = "Order:" + Guid.NewGuid().ToString("N");
        await opened.Store.AppendAsync(stream, [ContractEvent.Tick()]);
        var before = await SequenceValueAsync(opened.SchemaName);
        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            opened.Store.AppendAsync(stream, [ContractEvent.Tick()], expectedVersion: 0));
        Assert.Equal(before, await SequenceValueAsync(opened.SchemaName));
    }

    [Fact]
    public async Task KilledWriter_ReleasesTheReadHorizon()
    {
        var opened = await OpenAsync(requireTenantId: false);
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var slow = opened.Store.AppendAsync("slow", [ContractEvent.Tick()]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var fast = await opened.Store.AppendAsync("fast", [ContractEvent.Tick()]);
        Assert.Equal(1, await opened.Store.GetCurrentSequenceAsync());
        await opened.Hooks.KillOpenConnectionAsync();
        release.TrySetResult();
        await Assert.ThrowsAnyAsync<Exception>(() => slow.WaitAsync(TimeSpan.FromSeconds(15)));
        var head = await WaitForHeadAsync(opened.Store, fast.SequencePositions[0], TimeSpan.FromSeconds(10));
        Assert.Equal(fast.SequencePositions[0], head);
    }

    [Fact]
    public async Task SequenceJump_IsPassedWhenNoWriterIsOpen()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var stream = "Order:" + Guid.NewGuid().ToString("N");
        await opened.Store.AppendAsync(stream, [ContractEvent.Tick()]);
        await using var connection = await ConnectAsync();
        await using (var setval = new NpgsqlCommand(
            "SELECT setval(format('%I.event_position', @schema), 50)",
            connection))
        {
            setval.Parameters.AddWithValue("schema", opened.SchemaName);
            await setval.ExecuteScalarAsync();
        }

        var second = await opened.Store.AppendAsync(stream, [ContractEvent.Tick()]);
        Assert.Equal(51, second.SequencePositions[0]);
        var head = await WaitForHeadAsync(opened.Store, 51, TimeSpan.FromSeconds(5));
        Assert.Equal(51, head);
        var read = await opened.Log.ReadByQueryAsync(Query.All());
        Assert.Equal(new long[] { 1, 51 }, read.Events.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task UnrelatedOpenWrite_HoldsTheHorizonUntilGrace_ThenALateRowIsLogged()
    {
        var opened = await OpenAsync(requireTenantId: false, grace: TimeSpan.FromMilliseconds(300));
        await using var holder = await ConnectAsync();
        await using var hold = await holder.BeginTransactionAsync();
        await using (var xid = new NpgsqlCommand("SELECT pg_current_xact_id()", holder, hold))
            await xid.ExecuteScalarAsync();

        await opened.Store.AppendAsync("first", [ContractEvent.Tick()]);
        await using (var connection = await ConnectAsync())
        await using (var setval = new NpgsqlCommand(
            "SELECT setval(format('%I.event_position', @schema), 20)",
            connection))
        {
            setval.Parameters.AddWithValue("schema", opened.SchemaName);
            await setval.ExecuteScalarAsync();
        }

        var later = await opened.Store.AppendAsync("later", [ContractEvent.Tick()]);
        var head = await WaitForHeadAsync(opened.Store, later.SequencePositions[0], TimeSpan.FromSeconds(5));
        Assert.Equal(later.SequencePositions[0], head);
        Assert.Contains(opened.ReadLogEntries(), e => e.Level == LogLevel.Warning && e.Message.Contains("Read horizon advanced", StringComparison.Ordinal));

        await using var connection2 = await ConnectAsync();
        await using (var insert = new NpgsqlCommand(
            $"""
            INSERT INTO {Quote(opened.SchemaName)}.events (
                tenant_id, stream_id, version, global_position, event_id, event_type_id,
                schema_version, codec_id, bdata, partition_hash, recorded_at)
            SELECT 'default', 'late', 1, 10, gen_random_uuid(), id, 1, 2, E'\\x01'::bytea, 1, now()
            FROM {Quote(opened.SchemaName)}.event_types
            LIMIT 1
            """,
            connection2))
        {
            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        await opened.Store.GetCurrentSequenceAsync();
        var error = opened.ReadLogEntries().FirstOrDefault(e => e.Level == LogLevel.Error && e.Message.Contains("landed below a read horizon", StringComparison.Ordinal));
        Assert.Contains("10", error.Message, StringComparison.Ordinal);
        Assert.Contains("late", error.Message, StringComparison.Ordinal);
        await hold.RollbackAsync();
    }

    [Fact]
    public async Task DcbCheck_SeesTheAppendThatHeldTheTagLock()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var tag = "tag-" + Guid.NewGuid().ToString("N");
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var normal = opened.Store.AppendAsync("held", [ContractEvent.Tick()], tags: [tag]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var dcb = opened.Store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
            tags: [tag]);
        await Task.Delay(200);
        Assert.False(dcb.IsCompleted);
        release.TrySetResult();
        await normal.WaitAsync(TimeSpan.FromSeconds(15));
        await Assert.ThrowsAsync<ConcurrencyException>(() => dcb.WaitAsync(TimeSpan.FromSeconds(15)));
    }

    [Fact]
    public async Task TypesOnlyCondition_BlocksOtherAppends()
    {
        var opened = await OpenAsync(requireTenantId: false);
        await opened.Store.AppendAsync("seed", [new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var dcb = opened.Store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType("no.such.type.for.lock"))));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var blocked = opened.Store.AppendAsync("other", [ContractEvent.Tick()]);
        await Task.Delay(250);
        Assert.False(blocked.IsCompleted);
        release.TrySetResult();
        await dcb.WaitAsync(TimeSpan.FromSeconds(15));
        await blocked.WaitAsync(TimeSpan.FromSeconds(15));
    }

    [Fact]
    public async Task Payloads_RoundTripBinaryJsonAndReorderedKeys()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var nul = Encoding.UTF8.GetBytes("{\"note\":\"a\\u0000b\"}");
        var binary = new byte[] { 1, 2, 0, 3 };
        var stream = "Order:" + Guid.NewGuid().ToString("N");
        await opened.Log.AppendAsync(stream, [
            new AppendEvent("payload.nul", nul),
            new AppendEvent("payload.bin", binary, codecId: EventCodec.MemoryPack)
        ]);
        var rows = await opened.Log.ReadStreamAsync(stream);
        Assert.Equal(nul, rows[0].Payload.ToArray());
        Assert.Equal(EventCodec.Json, rows[0].CodecId);
        Assert.Equal(binary, rows[1].Payload.ToArray());
        Assert.Equal(EventCodec.MemoryPack, rows[1].CodecId);

        var typed = "Order:" + Guid.NewGuid().ToString("N");
        var tick = new ContractEventWithProperties(Guid.NewGuid(), DateTime.UtcNow, "sku", 4);
        await opened.Store.AppendAsync(typed, [tick]);
        var read = Assert.IsType<ContractEventWithProperties>(Assert.Single(await opened.Store.ReadStreamAsync(typed)).Event);
        Assert.Equal("sku", read.ProductId);
        Assert.Equal(4, read.Quantity);
    }

    [Fact]
    public async Task OrdinaryRole_CanReadTheSnapshotFunctions()
    {
        await using var connection = await ConnectAsync();
        await using (var role = new NpgsqlCommand(
            "SELECT rolsuper FROM pg_roles WHERE rolname = current_user", connection))
        {
            Assert.False((bool)(await role.ExecuteScalarAsync())!);
        }

        await using var command = new NpgsqlCommand(
            "SELECT pg_current_snapshot()::text, pg_xact_status(txid_current()::text::xid8)",
            connection);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Contains(':', reader.GetString(0));
        Assert.False(reader.IsDBNull(1));
    }

    [Fact]
    public async Task CaseSensitiveStreamIds_StaySeparate()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var id = Guid.NewGuid().ToString("N");
        await opened.Store.AppendAsync($"acme:Order:{id}", [ContractEvent.Tick()]);
        await opened.Store.AppendAsync($"acme:order:{id}", [ContractEvent.Tick()]);
        Assert.Single(await opened.Store.ReadStreamAsync($"acme:Order:{id}"));
        Assert.Single(await opened.Store.ReadStreamAsync($"acme:order:{id}"));
    }

    [Fact]
    public async Task Stress_CommittedEventsAreSeenOnceInOrder()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var duration = string.Equals(Environment.GetEnvironmentVariable("LAWNDART_PG_STRESS"), "long", StringComparison.OrdinalIgnoreCase)
            ? TimeSpan.FromSeconds(15)
            : TimeSpan.FromSeconds(2);
        var committed = new System.Collections.Concurrent.ConcurrentDictionary<long, byte>();
        var concurrency = 0;
        using var stop = new CancellationTokenSource(duration);
        var writers = Enumerable.Range(0, 16).Select(worker => Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                var stream = "w" + worker + ":" + Guid.NewGuid().ToString("N");
                try
                {
                    if (Random.Shared.Next(5) == 0)
                    {
                        await ((PostgresEventStore)opened.Store).AppendAndRollbackAsync(
                            stream,
                            [new AppendEvent("stress.rollback", "{}"u8.ToArray())],
                            CancellationToken.None);
                    }
                    else
                    {
                        var result = await opened.Store.AppendAsync(stream, [ContractEvent.Tick()], cancellationToken: CancellationToken.None);
                        committed.TryAdd(result.SequencePositions[0], 0);
                    }
                }
                catch (ConcurrencyException)
                {
                    Interlocked.Increment(ref concurrency);
                }

                await Task.Delay(Random.Shared.Next(0, 5));
            }
        })).ToArray();

        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                await AssertNoSkippedCommittedAsync(opened, committed);
                await Task.Delay(20);
            }
        })).ToArray();

        await Task.WhenAll(writers.Concat(readers));
        await AssertNoSkippedCommittedAsync(opened, committed);
        Assert.True(concurrency >= 0, $"concurrency conflicts: {concurrency}");
    }

    [Fact]
    public async Task LostReply_AfterTheStatementIsSent_ReturnsTheLandedAppend()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var store = (PostgresEventStore)opened.Store;
        var attempts = 0;
        store.AfterAppendStatement = _ =>
        {
            Interlocked.Increment(ref attempts);
            throw new NpgsqlException("connection reset after the statement was sent");
        };

        var stream = "Order:" + Guid.NewGuid().ToString("N");
        var result = await store.AppendAsync(stream, [ContractEvent.Tick()]);
        Assert.Equal(1, attempts);
        Assert.Single(await store.ReadStreamAsync(stream));
        Assert.Equal(result.SequencePositions[0], (await store.ReadStreamAsync(stream))[0].SequencePosition);
    }

    [Fact]
    public async Task DcbAppends_CrossMatchingTags_OneWins_AndOppositeOrderDoesNotDeadlock()
    {
        var opened = await OpenAsync(requireTenantId: false);
        for (var round = 0; round < 4; round++)
        {
            var left = "left-" + round + "-" + Guid.NewGuid().ToString("N");
            var right = "right-" + round + "-" + Guid.NewGuid().ToString("N");
            var firstTags = round % 2 == 0 ? new[] { left } : new[] { right };
            var secondTags = round % 2 == 0 ? new[] { right } : new[] { left };
            var first = opened.Store.AppendAsync(
                [ContractEvent.Tick()],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(secondTags))),
                tags: firstTags);
            var second = opened.Store.AppendAsync(
                [ContractEvent.Tick()],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(firstTags))),
                tags: secondTags);
            var results = await Task.WhenAll(SettleAsync(first), SettleAsync(second));
            Assert.Equal(1, results.Count(static item => item is null));
            Assert.Equal(1, results.Count(static item => item is ConcurrencyException));
        }
    }

    [Fact]
    public async Task TagConditionDcb_AndTypesOnlyDcb_OneWins()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var tag = "cross-" + Guid.NewGuid().ToString("N");
        var typeName = "backend-contract.test-event";
        var tagged = opened.Store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType(typeName))),
            tags: [tag]);
        var typesOnly = opened.Store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))));
        var results = await Task.WhenAll(SettleAsync(tagged), SettleAsync(typesOnly));
        Assert.Equal(1, results.Count(static item => item is null));
        Assert.Equal(1, results.Count(static item => item is ConcurrencyException));
    }

    [Fact]
    public async Task AppRole_CannotCreateTables_AndCanReadTheGapFunctions()
    {
        var opened = await OpenAsync(requireTenantId: false);
        await using var app = await ConnectAsync();
        var ex = await Assert.ThrowsAsync<PostgresException>(async () =>
        {
            await using var command = new NpgsqlCommand(
                $"CREATE TABLE {Quote(opened.SchemaName)}.not_allowed (id int)",
                app);
            await command.ExecuteNonQueryAsync();
        });
        Assert.Equal("42501", ex.SqlState);

        await using var owner = new NpgsqlConnection(_fixture.AdminConnectionString);
        await owner.OpenAsync();
        await using (var who = new NpgsqlCommand("SELECT rolsuper FROM pg_roles WHERE rolname = current_user", owner))
            Assert.True((bool)(await who.ExecuteScalarAsync())!);

        Assert.True(await opened.Store.GetCurrentSequenceAsync() >= 0);
        await using (var snap = new NpgsqlCommand("SELECT pg_current_snapshot()::text", app))
        {
            var text = (string)(await snap.ExecuteScalarAsync())!;
            Assert.Contains(':', text);
        }
    }

    [Fact]
    public async Task StreamId_LongerThan255_Throws()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var ok = new string('a', 255);
        await opened.Store.AppendAsync(ok, [ContractEvent.Tick()]);
        Assert.Single(await opened.Store.ReadStreamAsync(ok));
        var tooLong = new string('b', 256);
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => opened.Store.AppendAsync(tooLong, [ContractEvent.Tick()]));
        Assert.Contains("255", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TwoContexts_GetSeparateSchemas()
    {
        var services = new ServiceCollection();
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddBoundedContext("ordering")
            .UsePostgres(o => o.ConnectionString = _fixture.AdminConnectionString)
            .WithEventTypes<ContractTick>();
        services.AddBoundedContext("catalog")
            .UsePostgres(o => o.ConnectionString = _fixture.AdminConnectionString)
            .WithEventTypes<ContractTick>();
        await using var provider = services.BuildServiceProvider();
        Assert.Equal("ordering", provider.GetRequiredKeyedService<PostgresEventStoreOptions>("ordering").SchemaName);
        Assert.Equal("catalog", provider.GetRequiredKeyedService<PostgresEventStoreOptions>("catalog").SchemaName);

        var ordering = (PostgresEventStore)provider.GetRequiredKeyedService<IEventStore>("ordering");
        var catalog = (PostgresEventStore)provider.GetRequiredKeyedService<IEventStore>("catalog");
        await ordering.InitializeSchemaAsync();
        await catalog.InitializeSchemaAsync();
        var stream = "ordering:Order:" + Guid.NewGuid().ToString("N");
        await ordering.AppendAsync(stream, [ContractEvent.Tick()], metadata: new LawnDart.Metadata.EventMetadata { TenantId = "ordering" });
        Assert.Single(await ordering.ReadStreamAsync(stream));
        Assert.Empty(await catalog.ReadStreamAsync(stream));

        await using var connection = new NpgsqlConnection(_fixture.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT table_schema FROM information_schema.tables
            WHERE table_name = 'events' AND table_schema IN ('ordering', 'catalog')
            ORDER BY table_schema
            """,
            connection);
        var schemas = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            schemas.Add(reader.GetString(0));
        Assert.Equal(new[] { "catalog", "ordering" }, schemas);
    }

    private static async Task AssertNoSkippedCommittedAsync(
        ContractEventStore opened,
        System.Collections.Concurrent.ConcurrentDictionary<long, byte> committed)
    {
        var head = await opened.Store.GetCurrentSequenceAsync();
        var expected = committed.Keys.Where(position => position <= head).Order().ToArray();
        var read = await opened.Log.ReadByQueryAsync(Query.All(), toSequencePosition: head);
        var positions = read.Events.Select(e => e.SequencePosition).ToArray();
        Assert.Equal(positions.Order(), positions);
        Assert.Equal(positions.Length, positions.Distinct().Count());
        foreach (var position in expected)
            Assert.Contains(position, positions);
    }

    private static async Task<Exception?> SettleAsync(Task<AppendResult> append)
    {
        try
        {
            await append.WaitAsync(TimeSpan.FromSeconds(15));
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private async Task<ContractEventStore> OpenAsync(bool requireTenantId = false, TimeSpan? grace = null)
        => await _fixture.Backend.OpenEventStoreAsync(ContractStores.Isolated(
            requireTenantId: requireTenantId,
            readHorizonGracePeriod: grace));

    private async Task<NpgsqlConnection> ConnectAsync()
    {
        var connection = new NpgsqlConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    private async Task<string> TenantOfAsync(string schema, string streamId)
    {
        await using var connection = await ConnectAsync();
        await using var command = new NpgsqlCommand(
            $"SELECT tenant_id FROM {Quote(schema)}.events WHERE stream_id = @id",
            connection);
        command.Parameters.AddWithValue("id", streamId);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    private async Task<long> SequenceValueAsync(string schema)
    {
        await using var connection = await ConnectAsync();
        await using var command = new NpgsqlCommand($"SELECT last_value FROM {Quote(schema)}.event_position", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<List<string>> NamesAsync(NpgsqlConnection connection, string schema, string like)
    {
        await using var command = new NpgsqlCommand(
            "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = @schema AND c.relname LIKE @like",
            connection);
        command.Parameters.AddWithValue("schema", schema);
        command.Parameters.AddWithValue("like", like);
        var names = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            names.Add(reader.GetString(0));
        return names;
    }

    private static async Task<long> WaitForHeadAsync(IEventStore store, long expected, TimeSpan timeout)
    {
        var started = DateTime.UtcNow;
        long head = 0;
        while (DateTime.UtcNow - started < timeout)
        {
            head = await store.GetCurrentSequenceAsync();
            if (head >= expected && expected > 0)
                return head;
            if (expected == 0 && head == 0)
                return head;
            await Task.Delay(40);
        }

        return head;
    }

    private static string Stream(string tenant) => tenant + ":Order:" + Guid.NewGuid().ToString("N");

    private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}

public sealed class PostgresRegistrationTests
{
    [Fact]
    public void UsePostgres_EnableOutbox_IsRejected()
    {
        var services = new ServiceCollection();
        services.AddLawnDart();
        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddBoundedContext("default").UsePostgres(o =>
            {
                o.ConnectionString = "Host=localhost;Username=lawndart_app;Password=lawndart_app;Database=lawndart";
                o.EnableOutbox = true;
            }));
        Assert.Contains("EnableOutbox", ex.Message, StringComparison.Ordinal);
        Assert.Contains("false", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UsePostgres_NamedContext_UsesThatNameAsTheSchema()
    {
        var services = new ServiceCollection();
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddBoundedContext("ordering").UsePostgres(o =>
            o.ConnectionString = "Host=localhost;Username=u;Password=p;Database=lawndart");
        services.AddBoundedContext("default").UsePostgres(o =>
            o.ConnectionString = "Host=localhost;Username=u;Password=p;Database=lawndart");
        using var provider = services.BuildServiceProvider();
        Assert.Equal("ordering", provider.GetRequiredKeyedService<PostgresEventStoreOptions>("ordering").SchemaName);
        Assert.Equal("lawndart", provider.GetRequiredKeyedService<PostgresEventStoreOptions>("default").SchemaName);
    }
}
