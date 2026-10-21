using System.Security.Cryptography;
using System.Text.Json.Nodes;
using LawnDart.Dcb;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventSourcing.Postgres.Snapshots;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Postgres;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Partitioning;
using Npgsql;

namespace LawnDart.Backends.Contract.Tests;

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
public sealed class PostgresProjectionPhase3Tests
{
    private readonly PostgresContractFixture _fixture;

    public PostgresProjectionPhase3Tests(PostgresContractFixture fixture) => _fixture = fixture;

    [Fact(Timeout = 60000)]
    public async Task AtomicFlush_FailureAfterViews_LeavesBothUnchanged()
    {
        var options = Options(Schema());
        var (views, checkpoints) = await OpenStoresAsync(options);
        var checkpoint = NewCheckpoint("Flush:v1", 0, 4);
        await checkpoints.SaveViewsAndCheckpointAsync("Flush:v1", [("a", "{\"n\":1}", 4)], checkpoint);
        checkpoints.FailNextFlushAfterViews();
        var failed = NewCheckpoint("Flush:v1", 0, 9);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            checkpoints.SaveViewsAndCheckpointAsync("Flush:v1", [("a", "{\"n\":2}", 9)], failed));

        var view = await views.GetViewWithCheckpointAsync("Flush:v1", "a");
        Assert.NotNull(view);
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse("{\"n\":1}"), JsonNode.Parse(view.Value.ViewData)));
        Assert.Equal(4, view.Value.Checkpoint);
        Assert.Equal(4, (await checkpoints.GetCheckpointAsync("Flush:v1", 0))!.LastSequencePosition);
    }

    [Fact(Timeout = 60000)]
    public async Task SameNodeNumber_OneRuns_TheOtherWaitsAndWarns()
    {
        var options = Options(Schema());
        var (views, leaderStore) = await OpenStoresAsync(options);
        var waiterStore = new PostgresCheckpointStore(options, views);
        var leaderLog = new ListLogger<PostgresProjectionRunnerLock>();
        var waiterLog = new ListLogger<PostgresProjectionRunnerLock>();
        var leaderLock = new PostgresProjectionRunnerLock(leaderStore, options, leaderLog);
        var waiterLock = new PostgresProjectionRunnerLock(waiterStore, options, waiterLog);
        var leader = await leaderLock.AcquireAsync("OrderSummary:v1", 0);
        var pending = waiterLock.AcquireAsync("OrderSummary:v1", 0);
        try
        {
            await WaitUntilAsync(() => waiterLog.Entries.Any(entry =>
                entry.Level == Microsoft.Extensions.Logging.LogLevel.Warning
                && entry.Message.Contains("share node number", StringComparison.Ordinal)));
            Assert.True(leader.IsHeld);
            Assert.False(pending.IsCompleted);
            Assert.DoesNotContain(leaderLog.Entries, entry => entry.Message.Contains("share node number", StringComparison.Ordinal));
        }
        finally
        {
            await leader.DisposeAsync();
            var taken = await pending.WaitAsync(TimeSpan.FromSeconds(15));
            await taken.DisposeAsync();
        }
    }

    [Fact(Timeout = 90000)]
    public async Task DifferentNodeNumbers_RunSideBySide_GlobalReplicasStayPerNode()
    {
        var options = Options(Schema());
        var (views, checkpoints) = await OpenStoresAsync(options);
        var node0 = new PostgresProjectionRunnerLock(checkpoints, options);
        var node1 = new PostgresProjectionRunnerLock(checkpoints, options);
        var lead0 = await node0.AcquireAsync("ContractGlobal:v1", 0);
        var lead1 = await node1.AcquireAsync("ContractGlobal:v1", 1);
        Assert.True(lead0.IsHeld);
        Assert.True(lead1.IsHeld);
        Assert.Equal("global:n0", ProjectionInstanceIds.ForUnpartitioned(0, 2));
        Assert.Equal("global:n1", ProjectionInstanceIds.ForUnpartitioned(1, 2));

        var events = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        var reg = new ProjectionRegistration(
            typeof(ContractGlobalCounterProjection),
            typeof(ContractCounterView),
            "ContractGlobal",
            ProjectionKind.Global,
            TenantScope.TenantScoped);
        const int count = 4;
        for (var i = 0; i < count; i++)
        {
            await events.AppendAsync(
                "acct:Counter:" + i,
                [new ContractCounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: Meta());
        }

        var head = await events.GetCurrentSequenceAsync();
        var runner0 = Runner(reg, events, views, checkpoints, lead0, 0);
        var runner1 = Runner(reg, events, views, checkpoints, lead1, 1);
        await runner0.StartAsync(CancellationToken.None);
        await runner1.StartAsync(CancellationToken.None);
        try
        {
            await WaitForNodeAsync(checkpoints, reg.StorageKey, 0, head);
            await WaitForNodeAsync(checkpoints, reg.StorageKey, 1, head);
            var left = await views.GetViewAsync(reg.StorageKey, "global:n0");
            var right = await views.GetViewAsync(reg.StorageKey, "global:n1");
            Assert.True(JsonNode.DeepEquals(JsonNode.Parse(left), JsonNode.Parse(right)));
            Assert.True(JsonNode.DeepEquals(JsonNode.Parse($"{{\"count\":{count}}}"), JsonNode.Parse(left)));
        }
        finally
        {
            await runner0.StopAsync(CancellationToken.None);
            await runner1.StopAsync(CancellationToken.None);
            runner0.Dispose();
            runner1.Dispose();
            await lead0.DisposeAsync();
            await lead1.DisposeAsync();
        }
    }

    [Fact(Timeout = 90000)]
    public async Task DifferentNodeNumbers_SplitStreamsAcrossTheTwoCopies()
    {
        var options = Options(Schema());
        var (views, checkpoints) = await OpenStoresAsync(options);
        var node0 = new PostgresProjectionRunnerLock(checkpoints, options);
        var node1 = new PostgresProjectionRunnerLock(checkpoints, options);
        await using var lead0 = await node0.AcquireAsync("ContractCounter:v1", 0);
        await using var lead1 = await node1.AcquireAsync("ContractCounter:v1", 1);
        var part0 = new ConsistentHashPartitioningService(0, 2);
        var part1 = new ConsistentHashPartitioningService(1, 2);
        var streams = Enumerable.Range(0, 16).Select(i => "acct:Counter:split-" + i).ToArray();
        var owned0 = streams.Count(part0.OwnsStream);
        var owned1 = streams.Count(part1.OwnsStream);
        Assert.True(owned0 > 0);
        Assert.True(owned1 > 0);
        Assert.Equal(streams.Length, owned0 + owned1);

        var events = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        foreach (var streamId in streams)
        {
            await events.AppendAsync(
                streamId,
                [new ContractCounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: Meta());
        }

        var reg = new ProjectionRegistration(
            typeof(ContractCounterProjection),
            typeof(ContractCounterView),
            "ContractCounter",
            ProjectionKind.SingleStream,
            TenantScope.TenantScoped,
            streamType: "Counter");
        var head = await events.GetCurrentSequenceAsync();
        var runner0 = Runner(reg, events, views, checkpoints, lead0, 0, part0);
        var runner1 = Runner(reg, events, views, checkpoints, lead1, 1, part1);
        await runner0.StartAsync(CancellationToken.None);
        await runner1.StartAsync(CancellationToken.None);
        try
        {
            await WaitForNodeAsync(checkpoints, reg.StorageKey, 0, head);
            await WaitForNodeAsync(checkpoints, reg.StorageKey, 1, head);
            Assert.Equal(owned0, runner0.WorkingSetCount);
            Assert.Equal(owned1, runner1.WorkingSetCount);
            Assert.Equal(streams.Length, (await views.GetViewsByTypeAsync(reg.StorageKey)).Count());
        }
        finally
        {
            await runner0.StopAsync(CancellationToken.None);
            await runner1.StopAsync(CancellationToken.None);
            runner0.Dispose();
            runner1.Dispose();
        }
    }

    [Fact(Timeout = 60000)]
    public async Task KillingTheLockConnection_HandsOver_AndRejectsTheStaleSave()
    {
        var options = Options(Schema());
        var (views, leaderStore) = await OpenStoresAsync(options);
        var waiterStore = new PostgresCheckpointStore(options, views);
        var leaderLock = new PostgresProjectionRunnerLock(leaderStore, options);
        var waiterLog = new ListLogger<PostgresProjectionRunnerLock>();
        var waiterLock = new PostgresProjectionRunnerLock(waiterStore, options, waiterLog);
        var leader = await leaderLock.AcquireAsync("Cart:v1", 0);
        var pending = waiterLock.AcquireAsync("Cart:v1", 0);
        await WaitUntilAsync(() => waiterLog.Entries.Any(entry =>
            entry.Message.Contains("share node number", StringComparison.Ordinal)));
        Assert.NotNull(leader.SessionProcessId);

        await using (var connection = new NpgsqlConnection(_fixture.AdminConnectionString))
        {
            await connection.OpenAsync();
            await using var kill = new NpgsqlCommand("SELECT pg_terminate_backend(@pid)", connection);
            kill.Parameters.AddWithValue("pid", leader.SessionProcessId!.Value);
            await kill.ExecuteScalarAsync();
        }

        var taken = await pending.WaitAsync(TimeSpan.FromSeconds(20));
        try
        {
            Assert.True(taken.IsHeld);
            Assert.True(taken.Epoch > leader.Epoch);
            var stale = NewCheckpoint("Cart:v1", 0, 3);
            var rejected = await Assert.ThrowsAsync<ProjectionLeadershipLostException>(() =>
                leaderStore.SaveCheckpointAsync(stale));
            Assert.Contains("was not saved", rejected.Message, StringComparison.Ordinal);
            await waiterStore.SaveCheckpointAsync(NewCheckpoint("Cart:v1", 0, 3));
            Assert.Equal(3, (await waiterStore.GetCheckpointAsync("Cart:v1", 0))!.LastSequencePosition);
            await WaitUntilAsync(() => !leader.IsHeld);
            Assert.True(taken.IsHeld);
            Assert.False(leader.IsHeld);
        }
        finally
        {
            await leader.DisposeAsync();
            await taken.DisposeAsync();
        }
    }

    [Fact(Timeout = 60000)]
    public async Task SnapshotChecksum_MatchesStoredBytes_AndABadChecksumReplays()
    {
        var opened = await _fixture.Backend.OpenSnapshotsAsync(ContractStores.Isolated());
        var snapshots = (PostgresSnapshotStore)opened.Snapshots;
        var streamId = "acct:Order:" + Guid.NewGuid().ToString("N");
        var state = new UnitsState { Units = 7 };
        await snapshots.SaveSnapshotAsync(streamId, 2, 2, state);

        await using (var connection = new NpgsqlConnection(_fixture.AdminConnectionString))
        {
            await connection.OpenAsync();
            await using var read = new NpgsqlCommand(
                $"SELECT state_data, btrim(checksum) FROM {snapshots.QualifiedEventTable} WHERE stream_id = @id",
                connection);
            read.Parameters.AddWithValue("id", streamId);
            await using var reader = await read.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            var data = (byte[])reader.GetValue(0);
            var checksum = reader.GetString(1);
            Assert.Equal(Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant(), checksum);
        }

        await using (var connection = new NpgsqlConnection(_fixture.AdminConnectionString))
        {
            await connection.OpenAsync();
            await using var spoil = new NpgsqlCommand(
                $"UPDATE {snapshots.QualifiedEventTable} SET checksum = @checksum WHERE stream_id = @id",
                connection);
            spoil.Parameters.AddWithValue("checksum", new string('a', 64));
            spoil.Parameters.AddWithValue("id", streamId);
            Assert.Equal(1, await spoil.ExecuteNonQueryAsync());
        }

        var (loaded, info) = await snapshots.LoadSnapshotAsync<UnitsState>(streamId);
        Assert.Null(info);
        Assert.Null(loaded);
    }

    private PostgresProjectionStoreOptions Options(string schema)
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

    private static LightweightProjectionRunnerService Runner(
        ProjectionRegistration registration,
        IEventStore events,
        PostgresViewStore views,
        PostgresCheckpointStore checkpoints,
        IProjectionRunnerLease lease,
        int node,
        IPartitioningService? partitioning = null)
        => new(
            registration,
            events,
            views,
            checkpoints,
            partitioning ?? new SingleNodePartitioningService(),
            new LightweightProjectionOptions
            {
                NodeInstance = node,
                TotalInstances = 2,
                PollInterval = TimeSpan.FromMilliseconds(20),
                CheckpointInterval = 1,
                BatchSize = 50,
                SkipTailFlushWhileCatchingUp = false,
                UseSubscribe = false
            },
            runnerLock: LockAlreadyHeld(lease));

    private static IProjectionRunnerLock LockAlreadyHeld(IProjectionRunnerLease lease)
        => new HeldLock(lease);

    private static LawnDart.Projections.ProjectionCheckpoint NewCheckpoint(string type, int node, long position)
        => new()
        {
            ProjectionType = type,
            NodeId = node,
            LastSequencePosition = position,
            LastUpdated = DateTime.UtcNow,
            TotalEventsProcessed = position
        };

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static string Schema() => "p" + Guid.NewGuid().ToString("N");

    private static async Task WaitForNodeAsync(
        PostgresCheckpointStore checkpoints, string storageKey, int node, long position)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (true)
        {
            var checkpoint = await checkpoints.GetCheckpointAsync(storageKey, node, CancellationToken.None);
            if (checkpoint is not null && checkpoint.LastSequencePosition >= position)
                return;
            await Task.Delay(25, cts.Token);
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (!condition())
            await Task.Delay(20, cts.Token);
    }

    private sealed class HeldLock : IProjectionRunnerLock
    {
        private readonly IProjectionRunnerLease _lease;

        public HeldLock(IProjectionRunnerLease lease) => _lease = lease;

        public Task<IProjectionRunnerLease> AcquireAsync(string projectionType, int nodeId, CancellationToken cancellationToken = default)
            => Task.FromResult(_lease);
    }
}

public sealed class UnitsState
{
    public int Units { get; set; }
}

public sealed class ContractGlobalCounterProjection : LawnDart.Projections.Sdk.ProjectionBase<ContractCounterView>
{
    public void Handle(ContractCounterIncremented e) => State.Count += e.Amount;
}
