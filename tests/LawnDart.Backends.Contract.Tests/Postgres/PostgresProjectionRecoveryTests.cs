using System.Text.Json.Nodes;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Admin;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Postgres;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Partitioning;
using Npgsql;
using Testcontainers.PostgreSql;

namespace LawnDart.Backends.Contract.Tests;

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
public sealed class PostgresProjectionRecoveryTests
{
    private readonly PostgresContractFixture _fixture;

    public PostgresProjectionRecoveryTests(PostgresContractFixture fixture) => _fixture = fixture;

    [Fact(Timeout = 90000)]
    public async Task OneRunner_RecoversAfterItsLockSessionIsKilled()
    {
        var options = StoreOptions(Schema());
        var (views, checkpoints) = await OpenStoresAsync(options);
        var store = new PostgresProjectionStore(views, checkpoints);
        var events = NewEvents();
        var reg = Registration();
        var streamId = Stream();
        var runner = Runner(reg, events, store, store, new PostgresProjectionRunnerLock(checkpoints, options));
        await runner.StartAsync(CancellationToken.None);
        try
        {
            await AppendAsync(events, streamId, 3);
            await WaitForCheckpointAsync(checkpoints, reg.StorageKey, await events.GetCurrentSequenceAsync());
            await KillLeaderAsync();
            await AppendAsync(events, streamId, 3);
            await WaitForCheckpointAsync(checkpoints, reg.StorageKey, await events.GetCurrentSequenceAsync());
            Assert.False(runner.ExecuteTask!.IsCompleted);
            await AssertCountAsync(views, reg.StorageKey, streamId, 6);
        }
        finally
        {
            await runner.StopAsync(CancellationToken.None);
            runner.Dispose();
        }
    }

    [Fact(Timeout = 120000)]
    public async Task TwoRunners_HandBackTheLeadAfterEachLockSessionIsKilled()
    {
        var options = StoreOptions(Schema());
        var (views, storeA) = await OpenStoresAsync(options);
        var storeB = new PostgresCheckpointStore(options, views);
        var bundleA = new PostgresProjectionStore(views, storeA);
        var bundleB = new PostgresProjectionStore(views, storeB);
        var events = NewEvents();
        var reg = Registration();
        var streamId = Stream();
        var runnerA = Runner(reg, events, bundleA, bundleA, new PostgresProjectionRunnerLock(storeA, options));
        var runnerB = Runner(reg, events, bundleB, bundleB, new PostgresProjectionRunnerLock(storeB, options));
        await runnerA.StartAsync(CancellationToken.None);
        await runnerB.StartAsync(CancellationToken.None);
        try
        {
            await AppendAsync(events, streamId, 2);
            await WaitForCheckpointAsync(storeA, reg.StorageKey, await events.GetCurrentSequenceAsync());
            await KillLeaderAsync();
            await AppendAsync(events, streamId, 2);
            await WaitForCheckpointAsync(storeA, reg.StorageKey, await events.GetCurrentSequenceAsync());
            await KillLeaderAsync();
            await AppendAsync(events, streamId, 2);
            await WaitForCheckpointAsync(storeA, reg.StorageKey, await events.GetCurrentSequenceAsync());
            Assert.False(runnerA.ExecuteTask!.IsCompleted);
            Assert.False(runnerB.ExecuteTask!.IsCompleted);
            await AssertCountAsync(views, reg.StorageKey, streamId, 6);
        }
        finally
        {
            await runnerA.StopAsync(CancellationToken.None);
            await runnerB.StopAsync(CancellationToken.None);
            runnerA.Dispose();
            runnerB.Dispose();
        }
    }

    [Fact(Timeout = 60000)]
    public async Task StaleSave_AfterCheckpointDelete_IsRejected()
    {
        var options = StoreOptions(Schema());
        var (views, checkpoints) = await OpenStoresAsync(options);
        var lok = new PostgresProjectionRunnerLock(checkpoints, options);
        var leader = await lok.AcquireAsync("Cart:v1", 0);
        try
        {
            await checkpoints.SaveCheckpointAsync(Checkpoint("Cart:v1", 5));
            await views.SaveViewAsync("Cart:v1", "x", "{\"n\":1}", 5);
            await checkpoints.DeleteCheckpointAsync("Cart:v1", 0);
            await Assert.ThrowsAsync<ProjectionLeadershipLostException>(() =>
                checkpoints.SaveViewsAndCheckpointAsync(
                    "Cart:v1",
                    [("x", "{\"n\":9}", 9)],
                    Checkpoint("Cart:v1", 9)));
            Assert.Null(await checkpoints.GetCheckpointAsync("Cart:v1", 0));
            Assert.True(JsonNode.DeepEquals(JsonNode.Parse("{\"n\":1}"), JsonNode.Parse(await views.GetViewAsync("Cart:v1", "x"))));
        }
        finally
        {
            await leader.DisposeAsync();
        }
    }

    [Fact(Timeout = 180000)]
    public async Task Rebuild_WaitingCopySavesBetweenResetAndViewDelete_KeepsTheEventTotal()
    {
        const int total = 60;
        var options = StoreOptions(Schema());
        var (views, checkpoints) = await OpenStoresAsync(options);
        var events = NewEvents();
        var reg = Registration();
        var streamId = Stream();
        await AppendAsync(events, streamId, total);
        var runnerOptions = FastOptions();
        var bundle = new PostgresProjectionStore(views, checkpoints);
        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => Runner(r, events, bundle, bundle, new PostgresProjectionRunnerLock(checkpoints, options), runnerOptions));
        var admin = new BoundedContextProjectionAdmin(
            manager, bundle, bundle, new ProjectionRegistrationCatalog([reg]), new SingleNodePartitioningService());
        await manager.StartAsync(reg.StorageKey);
        try
        {
            await WaitForCountAsync(views, reg.StorageKey, streamId, total);
            var stored = await checkpoints.GetCheckpointAsync(reg.StorageKey, 0);
            Assert.NotNull(stored);
            var epoch = await LeaderEpochAsync(checkpoints.QualifiedTableName, reg.StorageKey);
            var viewJson = await views.GetViewAsync(reg.StorageKey, streamId);
            Assert.Equal(total, CountOf(viewJson));
            var waiter = new PostgresCheckpointStore(options, views);
            waiter.BindLeaderEpoch(reg.StorageKey, 0, epoch);
            Task? save = null;
            checkpoints.BeforeDeletingViews = async (connection, transaction, ct) =>
            {
                await using var position = new NpgsqlCommand(
                    $"SELECT last_position FROM {checkpoints.QualifiedTableName} WHERE projection_type = @type AND node_id = 0",
                    connection,
                    transaction);
                position.Parameters.AddWithValue("type", reg.StorageKey);
                Assert.Equal(-1L, Convert.ToInt64(await position.ExecuteScalarAsync(ct)));

                await using var stillThere = new NpgsqlCommand(
                    $"SELECT view_data::text FROM {views.QualifiedTableName} WHERE projection_type = @type AND instance_id = @id",
                    connection,
                    transaction);
                stillThere.Parameters.AddWithValue("type", reg.StorageKey);
                stillThere.Parameters.AddWithValue("id", streamId);
                Assert.Equal(total, CountOf((string)(await stillThere.ExecuteScalarAsync(ct))!));

                save = waiter.SaveViewsAndCheckpointAsync(
                    reg.StorageKey,
                    [(streamId, viewJson!, stored.LastSequencePosition)],
                    Checkpoint(reg.StorageKey, stored.LastSequencePosition));
                await Task.Delay(TimeSpan.FromMilliseconds(150), ct);
            };

            await admin.RebuildAsync("ContractCounter");
            Assert.NotNull(save);
            await Assert.ThrowsAsync<ProjectionLeadershipLostException>(() => save!);
            await WaitForCountAsync(views, reg.StorageKey, streamId, total);
            Assert.Equal(total, CountOf(await views.GetViewAsync(reg.StorageKey, streamId)));
            var head = await events.GetCurrentSequenceAsync();
            Assert.Equal(head, (await checkpoints.GetCheckpointAsync(reg.StorageKey, 0))!.LastSequencePosition);
        }
        finally
        {
            checkpoints.BeforeDeletingViews = null;
            await manager.StopAllAsync(CancellationToken.None);
        }
    }

    [Fact(Timeout = 120000)]
    public async Task Rebuild_WhileASecondCopyWaits_ReplaysWithoutLosingEvents()
    {
        var options = StoreOptions(Schema());
        var (views, checkpoints) = await OpenStoresAsync(options);
        var events = NewEvents();
        var reg = Registration();
        var streamId = Stream();
        await AppendAsync(events, streamId, 5);
        var runnerOptions = FastOptions();
        var bundle = new PostgresProjectionStore(views, checkpoints);
        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => Runner(r, events, bundle, bundle, new PostgresProjectionRunnerLock(checkpoints, options), runnerOptions));
        var admin = new BoundedContextProjectionAdmin(
            manager, bundle, bundle, new ProjectionRegistrationCatalog([reg]), new SingleNodePartitioningService());
        var waiterStore = new PostgresCheckpointStore(options, views);
        var waiterBundle = new PostgresProjectionStore(views, waiterStore);
        var waiter = Runner(reg, events, waiterBundle, waiterBundle, new PostgresProjectionRunnerLock(waiterStore, options), runnerOptions);
        await manager.StartAsync(reg.StorageKey);
        await waiter.StartAsync(CancellationToken.None);
        try
        {
            await WaitForCheckpointAsync(checkpoints, reg.StorageKey, await events.GetCurrentSequenceAsync());
            await admin.RebuildAsync("ContractCounter");
            await WaitForCountAsync(views, reg.StorageKey, streamId, 5);
            var before = CountOf(await views.GetViewAsync(reg.StorageKey, streamId));
            await AppendAsync(events, streamId, 1);
            await WaitForCountAsync(views, reg.StorageKey, streamId, before + 1);
            Assert.False(waiter.ExecuteTask!.IsCompleted);
        }
        finally
        {
            await manager.StopAllAsync(CancellationToken.None);
            await waiter.StopAsync(CancellationToken.None);
            waiter.Dispose();
        }
    }

    private PostgresProjectionStoreOptions StoreOptions(string schema)
        => new()
        {
            ConnectionString = _fixture.ConnectionString,
            MigrationConnectionString = _fixture.AdminConnectionString,
            SchemaName = schema,
            LockProbeInterval = TimeSpan.FromMilliseconds(100),
            LeaseDuration = TimeSpan.FromSeconds(1),
            LeaseRenewInterval = TimeSpan.FromMilliseconds(200)
        };

    private static async Task<(PostgresViewStore Views, PostgresCheckpointStore Checkpoints)> OpenStoresAsync(
        PostgresProjectionStoreOptions options)
    {
        var views = new PostgresViewStore(options);
        var checkpoints = new PostgresCheckpointStore(options, views);
        await views.InitializeSchemaAsync();
        await checkpoints.InitializeSchemaAsync();
        return (views, checkpoints);
    }

    private async Task KillLeaderAsync()
    {
        await using var connection = new NpgsqlConnection(_fixture.AdminConnectionString);
        await connection.OpenAsync();
        await using var find = new NpgsqlCommand(
            """
            SELECT l.pid
            FROM pg_locks l
            JOIN pg_stat_activity a ON a.pid = l.pid
            WHERE l.locktype = 'advisory' AND l.granted
              AND a.application_name = 'lawndart-projection-lock'
            LIMIT 1
            """,
            connection);
        var pid = await find.ExecuteScalarAsync();
        if (pid is null or DBNull)
            throw new InvalidOperationException("No projection lock session is granted.");
        await using var kill = new NpgsqlCommand("SELECT pg_terminate_backend(@pid)", connection);
        kill.Parameters.AddWithValue("pid", Convert.ToInt32(pid));
        await kill.ExecuteScalarAsync();
    }

    private static InMemoryEventStore NewEvents()
        => new(options: new InMemoryEventStoreOptions { RequireTenantId = false });

    private static ProjectionRegistration Registration()
        => new(
            typeof(ContractCounterProjection),
            typeof(ContractCounterView),
            "ContractCounter",
            ProjectionKind.SingleStream,
            TenantScope.TenantScoped,
            streamType: "Counter");

    private static LightweightProjectionRunnerService Runner(
        ProjectionRegistration registration,
        IEventStore events,
        LawnDart.Projections.Storage.IViewStore views,
        LawnDart.Projections.Checkpoints.ICheckpointStore checkpoints,
        IProjectionRunnerLock runnerLock,
        LightweightProjectionOptions? options = null)
        => new(
            registration,
            events,
            views,
            checkpoints,
            new SingleNodePartitioningService(),
            options ?? FastOptions(),
            runnerLock: runnerLock);

    private static LightweightProjectionOptions FastOptions()
        => new()
        {
            PollInterval = TimeSpan.FromMilliseconds(20),
            CheckpointInterval = 1,
            BatchSize = 50,
            SkipTailFlushWhileCatchingUp = false,
            UseSubscribe = false,
            LeadershipRetryDelay = TimeSpan.FromMilliseconds(50),
            LeadershipRetryMaxDelay = TimeSpan.FromMilliseconds(200)
        };

    private static async Task AppendAsync(IEventStore events, string streamId, int count)
    {
        for (var i = 0; i < count; i++)
        {
            await events.AppendAsync(
                streamId,
                [new ContractCounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "test" });
        }
    }

    private static LawnDart.Projections.ProjectionCheckpoint Checkpoint(string type, long position)
        => new()
        {
            ProjectionType = type,
            NodeId = 0,
            LastSequencePosition = position,
            LastUpdated = DateTime.UtcNow,
            TotalEventsProcessed = position
        };

    private static async Task WaitForCheckpointAsync(LawnDart.Projections.Checkpoints.ICheckpointStore checkpoints, string storageKey, long position)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (true)
        {
            var checkpoint = await checkpoints.GetCheckpointAsync(storageKey, 0, CancellationToken.None);
            if (checkpoint is not null && checkpoint.LastSequencePosition >= position)
                return;
            await Task.Delay(25, cts.Token);
        }
    }

    private static async Task WaitForCountAsync(PostgresViewStore views, string storageKey, string streamId, int count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (true)
        {
            var json = await views.GetViewAsync(storageKey, streamId, CancellationToken.None);
            if (json is not null && CountOf(json) == count)
                return;
            await Task.Delay(25, cts.Token);
        }
    }

    private static async Task AssertCountAsync(PostgresViewStore views, string storageKey, string streamId, int count)
    {
        var json = await views.GetViewAsync(storageKey, streamId);
        Assert.Equal(count, CountOf(json));
    }

    private static int CountOf(string? json)
        => JsonNode.Parse(json)!["count"]!.GetValue<int>();

    private async Task<long> LeaderEpochAsync(string checkpointTable, string projectionType)
    {
        await using var connection = new NpgsqlConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"SELECT leader_epoch FROM {checkpointTable} WHERE projection_type = @type AND node_id = 0",
            connection);
        command.Parameters.AddWithValue("type", projectionType);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    private static string Schema() => "r" + Guid.NewGuid().ToString("N");

    private static string Stream() => "acct:Counter:" + Guid.NewGuid().ToString("N");
}

[Trait("Category", "Integration")]
public sealed class PostgresProjectionRunnerRestartTests
{
    [Fact(Timeout = 180000)]
    public async Task OneRunner_RecoversAfterADatabaseRestart()
        => await RecoverAfterRestartAsync(PostgresRunnerLockMode.Auto);

    [Fact(Timeout = 180000)]
    public async Task LeaseMode_RecoversAfterADatabaseRestart()
        => await RecoverAfterRestartAsync(PostgresRunnerLockMode.Lease);

    private static async Task RecoverAfterRestartAsync(PostgresRunnerLockMode mode)
    {
        var hostPort = FreeTcpPort();
        await using var container = new PostgreSqlBuilder(PostgresTestImage.Current)
            .WithDatabase("lawndart")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .WithPortBinding(hostPort, 5432)
            .Build();
        await container.StartAsync();
        var connectionString = new NpgsqlConnectionStringBuilder
        {
            Host = "127.0.0.1",
            Port = hostPort,
            Username = "postgres",
            Password = "postgres",
            Database = "lawndart"
        }.ConnectionString;
        var options = new PostgresProjectionStoreOptions
        {
            ConnectionString = connectionString,
            SchemaName = "runner" + Guid.NewGuid().ToString("N")[..12],
            RunnerLockMode = mode,
            LockProbeInterval = TimeSpan.FromMilliseconds(100),
            LeaseDuration = TimeSpan.FromSeconds(1),
            LeaseRenewInterval = TimeSpan.FromMilliseconds(200)
        };
        var views = new PostgresViewStore(options);
        var checkpoints = new PostgresCheckpointStore(options, views);
        var store = new PostgresProjectionStore(views, checkpoints);
        await store.InitializeSchemaAsync();
        var events = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        var reg = new ProjectionRegistration(
            typeof(ContractCounterProjection),
            typeof(ContractCounterView),
            "ContractCounter",
            ProjectionKind.SingleStream,
            TenantScope.TenantScoped,
            streamType: "Counter");
        var streamId = "acct:Counter:" + Guid.NewGuid().ToString("N");
        var runner = new LightweightProjectionRunnerService(
            reg,
            events,
            store,
            store,
            new SingleNodePartitioningService(),
            new LightweightProjectionOptions
            {
                PollInterval = TimeSpan.FromMilliseconds(20),
                CheckpointInterval = 1,
                BatchSize = 50,
                SkipTailFlushWhileCatchingUp = false,
                UseSubscribe = false,
                LeadershipRetryDelay = TimeSpan.FromMilliseconds(50),
                LeadershipRetryMaxDelay = TimeSpan.FromMilliseconds(200)
            },
            runnerLock: new PostgresProjectionRunnerLock(checkpoints, options));
        await runner.StartAsync(CancellationToken.None);
        try
        {
            for (var i = 0; i < 3; i++)
            {
                await events.AppendAsync(
                    streamId,
                    [new ContractCounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                    metadata: new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "test" });
            }

            await WaitAsync(async () =>
            {
                var checkpoint = await checkpoints.GetCheckpointAsync(reg.StorageKey, 0);
                return checkpoint is not null && checkpoint.LastSequencePosition >= await events.GetCurrentSequenceAsync();
            });
            await container.StopAsync();
            await WaitAsync(() => Task.FromResult(runner.ExecuteTask is { IsCompleted: false }));
            await container.StartAsync();
            PostgresProjectionConnections.ClearIdle(connectionString);
            for (var i = 0; i < 3; i++)
            {
                await events.AppendAsync(
                    streamId,
                    [new ContractCounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                    metadata: new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "test" });
            }

            await WaitAsync(async () =>
            {
                var checkpoint = await checkpoints.GetCheckpointAsync(reg.StorageKey, 0);
                var json = await views.GetViewAsync(reg.StorageKey, streamId);
                return checkpoint is not null
                    && checkpoint.LastSequencePosition >= await events.GetCurrentSequenceAsync()
                    && json is not null
                    && JsonNode.Parse(json)!["count"]!.GetValue<int>() == 6;
            }, TimeSpan.FromSeconds(40));
            Assert.False(runner.ExecuteTask!.IsCompleted);
        }
        finally
        {
            await runner.StopAsync(CancellationToken.None);
            runner.Dispose();
        }
    }

    private static async Task WaitAsync(Func<Task<bool>> condition, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(20));
        while (true)
        {
            try
            {
                if (await condition())
                    return;
            }
            catch (NpgsqlException)
            {
                // The database restart closed a pooled connection. The next try opens a new one.
            }

            await Task.Delay(25, cts.Token);
        }
    }

    private static int FreeTcpPort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
