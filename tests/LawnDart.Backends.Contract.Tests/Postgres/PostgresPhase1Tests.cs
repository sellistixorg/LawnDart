using System.Text;
using LawnDart;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.Postgres.Outbox;
using LawnDart.EventStore;
using LawnDart.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
        await opened.Store.AppendAsync(StreamId.Parse(bare), [ContractEvent.Tick()]);
        await opened.Store.AppendAsync(StreamId.Parse(prefixed), [ContractEvent.Tick()]);

        Assert.Equal("default", await TenantOfAsync(opened.SchemaName, bare));
        Assert.Equal("acme", await TenantOfAsync(opened.SchemaName, prefixed));
        Assert.False(StreamId.TryParse(emptyPrefix, out _));

        var bareMeta = await opened.Store.GetStreamAsync(StreamId.Parse(bare));
        Assert.Null(bareMeta!.TenantId);
    }

    [Fact]
    public async Task DcbReservedStreamId_StoresTenantInTenantColumn()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var withTenant = StreamId.Dcb("acme");
        var withoutTenant = StreamId.Dcb();

        await opened.Store.AppendAsync(withTenant, [ContractEvent.Tick()]);
        await opened.Store.AppendAsync(withoutTenant, [ContractEvent.Tick()]);

        Assert.Equal("acme", await TenantOfAsync(opened.SchemaName, withTenant.ToString()));
        Assert.Equal("default", await TenantOfAsync(opened.SchemaName, withoutTenant.ToString()));

        var withTenantMeta = await opened.Store.GetStreamAsync(withTenant);
        var withoutTenantMeta = await opened.Store.GetStreamAsync(withoutTenant);
        Assert.Equal("acme", withTenantMeta!.TenantId);
        Assert.Null(withoutTenantMeta!.TenantId);
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
            await opened.Store.AppendAsync(StreamId.Parse(stream), [ContractEvent.Tick()]);

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
        await opened.Store.AppendAsync(StreamId.Parse(stream), [ContractEvent.Tick()]);
        var before = await SequenceValueAsync(opened.SchemaName);
        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            opened.Store.AppendAsync(StreamId.Parse(stream), [ContractEvent.Tick()], expectedVersion: 0));
        Assert.Equal(before, await SequenceValueAsync(opened.SchemaName));
    }

    [Fact]
    public async Task KilledWriter_ReleasesTheReadHorizon()
    {
        var opened = await OpenAsync(requireTenantId: false);
        await opened.Store.AppendAsync(StreamId.Parse("test:s1"), [ContractEvent.Tick()]);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var slow = opened.Store.AppendAsync(StreamId.Parse("test:slow"), [ContractEvent.Tick()]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var fast = await opened.Store.AppendAsync(StreamId.Parse("test:fast"), [ContractEvent.Tick()]);
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
        await opened.Store.AppendAsync(StreamId.Parse(stream), [ContractEvent.Tick()]);
        await using var connection = await ConnectAsync();
        await using (var setval = new NpgsqlCommand(
            "SELECT setval(format('%I.event_position', @schema), 50)",
            connection))
        {
            setval.Parameters.AddWithValue("schema", opened.SchemaName);
            await setval.ExecuteScalarAsync();
        }

        var second = await opened.Store.AppendAsync(StreamId.Parse(stream), [ContractEvent.Tick()]);
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

        await opened.Store.AppendAsync(StreamId.Parse("test:first"), [ContractEvent.Tick()]);
        await using (var connection = await ConnectAsync())
        await using (var setval = new NpgsqlCommand(
            "SELECT setval(format('%I.event_position', @schema), 20)",
            connection))
        {
            setval.Parameters.AddWithValue("schema", opened.SchemaName);
            await setval.ExecuteScalarAsync();
        }

        var later = await opened.Store.AppendAsync(StreamId.Parse("test:later"), [ContractEvent.Tick()]);
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
        var tag = LawnDart.EventStore.Tag.Parse("tag:" + Guid.NewGuid().ToString("N"));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var normal = opened.Store.AppendAsync(StreamId.Parse("test:held"), [new TaggedEvent(ContractEvent.Tick(), [tag])]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var dcb = opened.Store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [tag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))));
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
        await opened.Store.AppendAsync(StreamId.Parse("test:seed"), [new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var dcb = opened.Store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType("no.such.type.for.lock"))));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var blocked = opened.Store.AppendAsync(StreamId.Parse("test:other"), [ContractEvent.Tick()]);
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
        await opened.Log.AppendAsync(StreamId.Parse(stream), [
            new AppendEvent("payload.nul", nul),
            new AppendEvent("payload.bin", binary, codecId: EventCodec.MemoryPack)
        ]);
        var rows = await opened.Log.ReadStreamAsync(StreamId.Parse(stream));
        Assert.Equal(nul, rows[0].Payload.ToArray());
        Assert.Equal(EventCodec.Json, rows[0].CodecId);
        Assert.Equal(binary, rows[1].Payload.ToArray());
        Assert.Equal(EventCodec.MemoryPack, rows[1].CodecId);

        var typed = "Order:" + Guid.NewGuid().ToString("N");
        var tick = new ContractEventWithProperties(Guid.NewGuid(), DateTime.UtcNow, "sku", 4);
        await opened.Store.AppendAsync(StreamId.Parse(typed), [tick]);
        var read = Assert.IsType<ContractEventWithProperties>(Assert.Single(await opened.Store.ReadStreamAsync(StreamId.Parse(typed))).Event);
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
        await opened.Store.AppendAsync(StreamId.Parse($"acme:Order:{id}"), [ContractEvent.Tick()]);
        await opened.Store.AppendAsync(StreamId.Parse($"acme:order:{id}"), [ContractEvent.Tick()]);
        Assert.Single(await opened.Store.ReadStreamAsync(StreamId.Parse($"acme:Order:{id}")));
        Assert.Single(await opened.Store.ReadStreamAsync(StreamId.Parse($"acme:order:{id}")));
    }

    [Fact]
    public async Task Stress_CommittedEventsAreSeenOnceInOrder()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var duration = string.Equals(Environment.GetEnvironmentVariable("LAWNDART_PG_STRESS"), "long", StringComparison.OrdinalIgnoreCase)
            ? TimeSpan.FromSeconds(15)
            : TimeSpan.FromSeconds(2);
        var committed = new System.Collections.Concurrent.ConcurrentDictionary<long, byte>();
        using var stop = new CancellationTokenSource(duration);
        var writers = Enumerable.Range(0, 16).Select(worker => Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                var stream = "test:w" + worker + ":" + Guid.NewGuid().ToString("N");
                if (Random.Shared.Next(5) == 0)
                {
                    await ((PostgresEventStore)opened.Store).AppendAndRollbackAsync(
                        stream,
                        [new AppendEvent("stress.rollback", "{}"u8.ToArray())],
                        CancellationToken.None);
                }
                else
                {
                    var result = await opened.Store.AppendAsync(StreamId.Parse(stream), [ContractEvent.Tick()], cancellationToken: CancellationToken.None);
                    committed.TryAdd(result.SequencePositions[0], 0);
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
        var result = await store.AppendAsync(StreamId.Parse(stream), [ContractEvent.Tick()]);
        Assert.Equal(1, attempts);
        Assert.Single(await store.ReadStreamAsync(StreamId.Parse(stream)));
        Assert.Equal(result.SequencePositions[0], (await store.ReadStreamAsync(StreamId.Parse(stream)))[0].SequencePosition);
    }

    [Fact]
    public async Task DcbAppends_CrossMatchingTags_OneWins()
    {
        var opened = await OpenAsync(requireTenantId: false);
        await HoldCrossTagsAsync(
            opened,
            writeTag: "left:" + Guid.NewGuid().ToString("N"),
            checkTag: "right:" + Guid.NewGuid().ToString("N"));
        await HoldCrossTagsAsync(
            opened,
            writeTag: "right:" + Guid.NewGuid().ToString("N"),
            checkTag: "left:" + Guid.NewGuid().ToString("N"));
    }

    [Fact]
    public async Task TagConditionDcb_AndTypesOnlyDcb_OneWins()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var typeName = "backend-contract.test-event";
        var writtenByTypesOnly = LawnDart.EventStore.Tag.Parse("types:" + Guid.NewGuid().ToString("N"));
        var checkedByTagCondition = "tag:" + Guid.NewGuid().ToString("N");

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var typesOnly = opened.Store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [writtenByTypesOnly])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType(typeName))));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var tagged = opened.Store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(writtenByTypesOnly))));
        await Task.Delay(200);
        Assert.False(tagged.IsCompleted);
        release.TrySetResult();
        await typesOnly.WaitAsync(TimeSpan.FromSeconds(15));
        await Assert.ThrowsAsync<ConcurrencyException>(() => tagged.WaitAsync(TimeSpan.FromSeconds(15)));

        entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        tagged = opened.Store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(Tag.Parse(checkedByTagCondition)))));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        typesOnly = opened.Store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType(typeName))));
        await Task.Delay(200);
        Assert.False(typesOnly.IsCompleted);
        release.TrySetResult();
        await tagged.WaitAsync(TimeSpan.FromSeconds(15));
        await Assert.ThrowsAsync<ConcurrencyException>(() => typesOnly.WaitAsync(TimeSpan.FromSeconds(15)));
    }

    [Fact]
    public async Task MixedCondition_TypeItemPlusTag_WaitsForAnOpenAppend()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var tag = LawnDart.EventStore.Tag.Parse("mix:" + Guid.NewGuid().ToString("N"));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var normal = opened.Store.AppendAsync(StreamId.Parse("test:held"), [ContractEvent.Tick()]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var dcb = opened.Store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [tag])],
            AppendCondition.FailIfMatches(Query.FromItems(
                QueryItem.ByType("backend-contract.test-event"),
                QueryItem.ByTags(tag))));
        await Task.Delay(200);
        Assert.False(dcb.IsCompleted);
        release.TrySetResult();
        await normal.WaitAsync(TimeSpan.FromSeconds(15));
        await Assert.ThrowsAsync<ConcurrencyException>(() => dcb.WaitAsync(TimeSpan.FromSeconds(15)));
    }

    [Fact]
    public async Task LostReply_WhenCancelledAfterSend_DoesNotSurfaceCancellation()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var store = (PostgresEventStore)opened.Store;
        store.AfterAppendStatement = _ =>
            throw new OperationCanceledException("cancelled after the statement was sent");

        var stream = "Order:" + Guid.NewGuid().ToString("N");
        var result = await store.AppendAsync(StreamId.Parse(stream), [ContractEvent.Tick()]);
        Assert.Equal(result.SequencePositions[0], (await store.ReadStreamAsync(StreamId.Parse(stream)))[0].SequencePosition);

        store.FailAppendReadBack = true;
        var ex = await Assert.ThrowsAsync<AppendOutcomeUnknownException>(() =>
            store.AppendAsync(StreamId.Parse("Order:" + Guid.NewGuid().ToString("N")), [ContractEvent.Tick()]));
        Assert.Contains("could not tell whether it was saved", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LostReply_WhenReadBackFails_ThrowsUnknown_AndDoesNotRetry()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var store = (PostgresEventStore)opened.Store;
        var attempts = 0;
        store.FailAppendReadBack = true;
        store.AfterAppendStatement = _ =>
        {
            Interlocked.Increment(ref attempts);
            throw new NpgsqlException("connection reset after the statement was sent");
        };

        var ex = await Assert.ThrowsAsync<AppendOutcomeUnknownException>(() =>
            store.AppendAsync(StreamId.Parse("Order:" + Guid.NewGuid().ToString("N")), [ContractEvent.Tick()]));
        Assert.Equal(1, attempts);
        Assert.Contains("could not tell whether it was saved", ex.Message, StringComparison.Ordinal);

        var calls = 0;
        store.AfterAppendStatement = null;
        store.CommitHook = (_, _) =>
        {
            Interlocked.Increment(ref calls);
            throw new NpgsqlException("commit connection reset");
        };
        var ex2 = await Assert.ThrowsAsync<AppendOutcomeUnknownException>(() =>
            store.AppendAsync(StreamId.Parse("Order:" + Guid.NewGuid().ToString("N")), [ContractEvent.Tick()]));
        Assert.Equal(1, calls);
        Assert.Contains("could not tell whether it was saved", ex2.Message, StringComparison.Ordinal);
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
        var ok = StreamId.Parse("a:" + new string('a', 253));
        Assert.Equal(255, ok.ToString().Length);
        await opened.Store.AppendAsync(ok, [ContractEvent.Tick()]);
        Assert.Single(await opened.Store.ReadStreamAsync(ok));
        var tooLong = "b:" + new string('b', 254);
        var ex = Assert.Throws<FormatException>(() => StreamId.Parse(tooLong));
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
        await ordering.AppendAsync(StreamId.Parse(stream), [ContractEvent.Tick()], metadata: new LawnDart.Metadata.EventMetadata { TenantId = "ordering" });
        Assert.Single(await ordering.ReadStreamAsync(StreamId.Parse(stream)));
        Assert.Empty(await catalog.ReadStreamAsync(StreamId.Parse(stream)));

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

    [Fact]
    public async Task UsePostgres_OutboxOff_DoesNotRunTranslations()
    {
        var context = "p" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddBoundedContext(context)
            .WithEventTypes(typeof(CounterCreated))
            .PublishEvents(events => events.Translate<CounterCreated, BrokenPostgresNotice>(_ =>
                throw new InvalidOperationException("broken translation")))
            .UsePostgres(o =>
            {
                o.ConnectionString = _fixture.AdminConnectionString;
                o.RequireTenantId = false;
            });
        await using var provider = services.BuildServiceProvider();
        var store = (PostgresEventStore)provider.GetRequiredKeyedService<IEventStore>(context);
        await store.InitializeSchemaAsync();
        var id = Guid.NewGuid();
        var stream = Stream("acme");

        await store.AppendAsync(stream, [new CounterCreated(id, DateTime.UtcNow, Guid.NewGuid())]);

        Assert.Equal(id, Assert.Single(await store.ReadStreamAsync(stream)).Event.Id);
    }

    [EventTypeName("postgres-registration.broken-notice")]
    private sealed record BrokenPostgresNotice(Guid Id, DateTime Timestamp) : IEvent;

    [Fact]
    public async Task FirstRead_StartsFromCommittedHead_WithoutScanningFromZero()
    {
        var opened = await OpenAsync(requireTenantId: false);
        var stream = Stream("acme");
        var batch = Enumerable.Range(0, 200).Select(_ => ContractEvent.Tick()).ToArray();
        await opened.Store.AppendAsync(stream, batch);

        // Create more than 16 historical sequence jumps so a scan from 0 could not reach the tail in one resolve.
        await using (var connection = await ConnectAsync())
        {
            long pos = 200;
            for (var i = 0; i < 20; i++)
            {
                pos += 10;
                await using var setval = new NpgsqlCommand(
                    "SELECT setval(format('%I.event_position', @schema), @val)",
                    connection);
                setval.Parameters.AddWithValue("schema", opened.SchemaName);
                setval.Parameters.AddWithValue("val", pos);
                await setval.ExecuteScalarAsync();
                await opened.Store.AppendAsync(stream, [ContractEvent.Tick()]);
                pos++;
            }
        }

        var expectedHead = await SequenceValueAsync(opened.SchemaName);
        var fresh = new PostgresEventStore(
            _fixture.ConnectionString,
            options: new PostgresEventStoreOptions
            {
                ConnectionString = _fixture.ConnectionString,
                SchemaName = opened.SchemaName,
                RequireTenantId = false
            });
        var sampledMarks = new List<long>();
        fresh.Horizon.OnSampleRead = sampledMarks.Add;

        var head = await fresh.GetCurrentSequenceAsync();
        Assert.Equal(expectedHead, head);
        Assert.Equal(expectedHead, fresh.Horizon.Mark);
        Assert.DoesNotContain(0L, sampledMarks);

        var read = await ((IEventLog)fresh).ReadByQueryAsync(Query.All());
        Assert.Equal(220, read.Events.Count);
        Assert.Equal(expectedHead, read.Events[^1].SequencePosition);
    }

    [Fact]
    public async Task FirstRead_DoesNotSkipPausedConcurrentAppend_AndResumesInOrder()
    {
        var opened = await OpenAsync(requireTenantId: false, grace: TimeSpan.FromSeconds(10));
        await opened.Store.AppendAsync(StreamId.Parse("test:seed"), [ContractEvent.Tick(), ContractEvent.Tick(), ContractEvent.Tick()]);
        await using (var connection = await ConnectAsync())
        await using (var setval = new NpgsqlCommand(
            "SELECT setval(format('%I.event_position', @schema), 1050)",
            connection))
        {
            setval.Parameters.AddWithValue("schema", opened.SchemaName);
            await setval.ExecuteScalarAsync();
        }

        var tailSeed = await opened.Store.AppendAsync(StreamId.Parse("test:seed-tail"), [ContractEvent.Tick()]);

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var slow = opened.Store.AppendAsync(StreamId.Parse("test:slow"), [ContractEvent.Tick()]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var fast = await opened.Store.AppendAsync(StreamId.Parse("test:fast"), [ContractEvent.Tick()]);

        var fresh = new PostgresEventStore(
            _fixture.ConnectionString,
            options: new PostgresEventStoreOptions
            {
                ConnectionString = _fixture.ConnectionString,
                SchemaName = opened.SchemaName,
                RequireTenantId = false,
                ReadHorizonGracePeriod = TimeSpan.FromSeconds(10)
            });
        var sampledMarks = new List<long>();
        fresh.Horizon.OnSampleRead = sampledMarks.Add;

        var heldHead = await fresh.GetCurrentSequenceAsync();
        Assert.True(heldHead < fast.SequencePositions[0], $"Expected head below {fast.SequencePositions[0]}, got {heldHead}");
        var during = await ((IEventLog)fresh).ReadByQueryAsync(Query.All());
        Assert.DoesNotContain(during.Events, e => e.SequencePosition == fast.SequencePositions[0]);

        release.TrySetResult();
        var slowResult = await slow.WaitAsync(TimeSpan.FromSeconds(15));

        var resolvedHead = await WaitForHeadAsync(fresh, fast.SequencePositions[0], TimeSpan.FromSeconds(10));
        Assert.Equal(fast.SequencePositions[0], resolvedHead);
        Assert.DoesNotContain(0L, sampledMarks);

        var after = await ((IEventLog)fresh).ReadByQueryAsync(Query.All());
        Assert.Equal(
            new long[] { 1, 2, 3, tailSeed.SequencePositions[0], slowResult.SequencePositions[0], fast.SequencePositions[0] },
            after.Events.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task FirstRead_WithActiveConcurrentWriters_NeverSkipsCommittedEvents()
    {
        var opened = await OpenAsync(requireTenantId: false, grace: TimeSpan.FromSeconds(10));
        for (var i = 0; i < 50; i++)
            await opened.Store.AppendAsync(StreamId.Parse("seed:" + i), [ContractEvent.Tick()]);

        var committed = new System.Collections.Concurrent.ConcurrentDictionary<long, byte>();
        for (long pos = 1; pos <= 50; pos++)
            committed[pos] = 0;

        using var stop = new CancellationTokenSource();
        var writerTasks = Enumerable.Range(0, 8).Select(worker => Task.Run(async () =>
        {
            var stream = $"w:{worker}-{Guid.NewGuid():N}";
            while (!stop.IsCancellationRequested)
            {
                var result = await opened.Store.AppendAsync(StreamId.Parse(stream), [ContractEvent.Tick()], cancellationToken: CancellationToken.None);
                committed[result.SequencePositions[0]] = 0;
            }
        })).ToArray();

        for (var round = 0; round < 6; round++)
        {
            await Task.Delay(30);
            var fresh = new PostgresEventStore(
                _fixture.ConnectionString,
                options: new PostgresEventStoreOptions
                {
                    ConnectionString = _fixture.ConnectionString,
                    SchemaName = opened.SchemaName,
                    RequireTenantId = false,
                    ReadHorizonGracePeriod = TimeSpan.FromSeconds(10)
                });
            var head = await fresh.GetCurrentSequenceAsync();
            var expected = committed.Keys.Where(p => p <= head).Order().ToArray();
            var read = await ((IEventLog)fresh).ReadByQueryAsync(Query.All(), toSequencePosition: head);
            var positions = read.Events.Select(e => e.SequencePosition).ToArray();
            Assert.Equal(positions.Order(), positions);
            Assert.Equal(positions.Length, positions.Distinct().Count());
            foreach (var position in expected)
                Assert.Contains(position, positions);
        }

        stop.Cancel();
        await Task.WhenAll(writerTasks);

        var finalReader = new PostgresEventStore(
            _fixture.ConnectionString,
            options: new PostgresEventStoreOptions
            {
                ConnectionString = _fixture.ConnectionString,
                SchemaName = opened.SchemaName,
                RequireTenantId = false
            });
        var finalHead = await finalReader.GetCurrentSequenceAsync();
        var maxCommitted = committed.Keys.Max();
        Assert.Equal(maxCommitted, finalHead);
        var finalRead = await ((IEventLog)finalReader).ReadByQueryAsync(Query.All());
        Assert.Equal(committed.Keys.Order(), finalRead.Events.Select(e => e.SequencePosition));
    }

    [Fact]
    public async Task FirstRead_HeldPastGrace_AdvancesWithWarning_AndLogsLateRow()
    {
        var opened = await OpenAsync(requireTenantId: false, grace: TimeSpan.FromMilliseconds(250));
        await opened.Store.AppendAsync(StreamId.Parse("test:seed"), [ContractEvent.Tick(), ContractEvent.Tick()]);

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var slow = opened.Store.AppendAsync(StreamId.Parse("test:slow"), [ContractEvent.Tick()]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var fast = await opened.Store.AppendAsync(StreamId.Parse("test:fast"), [ContractEvent.Tick()]);

        var logger = new ListLogger<PostgresEventStore>();
        var fresh = new PostgresEventStore(
            _fixture.ConnectionString,
            options: new PostgresEventStoreOptions
            {
                ConnectionString = _fixture.ConnectionString,
                SchemaName = opened.SchemaName,
                RequireTenantId = false,
                ReadHorizonGracePeriod = TimeSpan.FromMilliseconds(250)
            },
            logger: logger);

        var head = await WaitForHeadAsync(fresh, fast.SequencePositions[0], TimeSpan.FromSeconds(5));
        Assert.Equal(fast.SequencePositions[0], head);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("Read horizon advanced", StringComparison.Ordinal));

        release.TrySetResult();
        var slowResult = await slow.WaitAsync(TimeSpan.FromSeconds(15));
        await fresh.GetCurrentSequenceAsync();
        var late = logger.Entries.FirstOrDefault(e => e.Level == LogLevel.Error && e.Message.Contains("landed below a read horizon", StringComparison.Ordinal));
        Assert.Contains(slowResult.SequencePositions[0].ToString(System.Globalization.CultureInfo.InvariantCulture), late.Message, StringComparison.Ordinal);
        Assert.Contains("slow", late.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FirstRead_Subscription_DeliversHiddenCommitWithoutBackupPoll()
    {
        var opened = await OpenAsync(requireTenantId: false);
        await opened.Store.AppendAsync(StreamId.Parse("test:seed"), [ContractEvent.Tick()]);
        await using (var connection = await ConnectAsync())
        await using (var setval = new NpgsqlCommand(
            "SELECT setval(format('%I.event_position', @schema), 1050)",
            connection))
        {
            setval.Parameters.AddWithValue("schema", opened.SchemaName);
            await setval.ExecuteScalarAsync();
        }

        var tailSeed = await opened.Store.AppendAsync(StreamId.Parse("test:seed-tail"), [ContractEvent.Tick()]);

        await using var holder = await ConnectAsync();
        await using var transaction = await holder.BeginTransactionAsync();
        await using (var current = new NpgsqlCommand("SELECT pg_current_xact_id()", holder, transaction))
            await current.ExecuteScalarAsync();
        await using (var next = new NpgsqlCommand(
            $"SELECT nextval('{Quote(opened.SchemaName)}.event_position')", holder, transaction))
            await next.ExecuteScalarAsync();

        var appended = await opened.Store.AppendAsync(StreamId.Parse("test:fast"), [ContractEvent.Tick()]);
        var fresh = new PostgresEventStore(
            _fixture.ConnectionString,
            options: new PostgresEventStoreOptions
            {
                ConnectionString = _fixture.ConnectionString,
                SchemaName = opened.SchemaName,
                RequireTenantId = false,
                BackupPollInterval = TimeSpan.FromSeconds(30),
                PollOnlyInterval = TimeSpan.FromSeconds(30),
                ReadHorizonGracePeriod = TimeSpan.FromMinutes(5)
            });

        await using var handle = ((IEventLogSubscriptions)fresh).Subscribe("fresh-hidden", fromSequence: 1);
        using (var duringHold = new CancellationTokenSource(TimeSpan.FromMilliseconds(350)))
        {
            var sawEarly = false;
            try
            {
                sawEarly = await handle.Events.WaitToReadAsync(duringHold.Token);
            }
            catch (OperationCanceledException)
            {
                sawEarly = false;
            }

            Assert.False(sawEarly);
        }

        await transaction.RollbackAsync();
        var received = new List<RecordedEvent>();
        using var readTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        while (received.Count < 3)
        {
            Assert.True(await handle.Events.WaitToReadAsync(readTimeout.Token));
            while (handle.Events.TryRead(out var item))
                received.Add(item);
        }

        Assert.Equal(
            new long[] { 1, tailSeed.SequencePositions[0], appended.SequencePositions[0] },
            received.Select(e => e.SequencePosition));
    }

    private sealed class ListLogger<T> : ILogger<T>
    {
        private readonly object _sync = new();
        private readonly List<(LogLevel Level, string Message)> _entries = [];

        public IReadOnlyList<(LogLevel Level, string Message)> Entries
        {
            get { lock (_sync) return _entries.ToArray(); }
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_sync)
                _entries.Add((logLevel, formatter(state, exception)));
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose() { }
        }
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

    private static async Task HoldCrossTagsAsync(ContractEventStore opened, string writeTag, string checkTag)
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var held = opened.Store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [Tag.Parse(writeTag)])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(Tag.Parse(checkTag)))));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var waiter = opened.Store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [Tag.Parse(checkTag)])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(Tag.Parse(writeTag)))));
        await Task.Delay(200);
        Assert.False(waiter.IsCompleted);
        release.TrySetResult();
        await held.WaitAsync(TimeSpan.FromSeconds(15));
        await Assert.ThrowsAsync<ConcurrencyException>(() => waiter.WaitAsync(TimeSpan.FromSeconds(15)));
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
        command.Parameters.AddWithValue("id", streamId.ToString());
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

    private static StreamId Stream(string tenant) => StreamId.For(tenant, "Order", Guid.NewGuid().ToString("N"));

    private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}

public sealed class PostgresRegistrationTests
{
    [Fact]
    public void UsePostgres_EnableOutbox_RegistersTheWriter()
    {
        var services = new ServiceCollection();
        services.AddLawnDart();
        services.AddBoundedContext("default").UsePostgres(o =>
        {
            o.ConnectionString = "Host=localhost;Username=lawndart_app;Password=lawndart_app;Database=lawndart";
            o.EnableOutbox = true;
        });
        using var provider = services.BuildServiceProvider();
        var writer = provider.GetRequiredKeyedService<IOutboxWriter>("default");
        Assert.IsType<PostgresOutboxWriter>(writer);
        var options = provider.GetRequiredKeyedService<PostgresEventStoreOptions>("default");
        Assert.True(options.EnableOutbox);
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

    [Fact]
    public void UsePostgres_EnableOutbox_WithoutPublishEvents_ThrowsOnStoreResolve()
    {
        var services = new ServiceCollection();
        services.AddLawnDart();
        services.AddBoundedContext("ordering")
            .WithEventTypes(typeof(CounterCreated))
            .UsePostgres(o =>
            {
                o.ConnectionString = "Host=localhost;Username=lawndart_app;Password=lawndart_app;Database=lawndart";
                o.EnableOutbox = true;
            });
        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(
            () => provider.GetRequiredKeyedService<IEventStore>("ordering"));

        Assert.Contains("ordering", ex.Message, StringComparison.Ordinal);
        Assert.Contains("counter-created", ex.Message, StringComparison.Ordinal);
    }
}
