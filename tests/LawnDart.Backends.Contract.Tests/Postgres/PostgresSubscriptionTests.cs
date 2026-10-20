using System.Diagnostics;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventStore;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Npgsql;
using Testcontainers.PostgreSql;

namespace LawnDart.Backends.Contract.Tests;

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
public sealed class PostgresSubscriptionTests
{
    private readonly PostgresContractFixture _fixture;

    public PostgresSubscriptionTests(PostgresContractFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task WakeUp_WithLongBackupPoll_Delivers()
    {
        var store = await OpenStoreAsync(NewSchema(), configure: options =>
        {
            options.BackupPollInterval = TimeSpan.FromSeconds(30);
            options.PollOnlyInterval = TimeSpan.FromSeconds(30);
        });
        var stream = Stream();
        await using var handle = store.Subscribe("wake", fromSequence: 1);
        await WaitUntilAsync(() => store.Listener.Mode == PostgresListenMode.Listening && store.Listener.IsWaitingForNotification);
        await WaitForWaitersAsync(store.NewEvents, 1);
        var mark = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        store.BeforeAppendCommit = _ =>
        {
            mark.TrySetResult(Stopwatch.GetTimestamp());
            return Task.CompletedTask;
        };
        var read = ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(2));
        await store.AppendAsync(stream, [ContractEvent.Tick()]);
        var received = await read;
        var elapsed = Stopwatch.GetElapsedTime(await mark.Task);
        Assert.True(elapsed < TimeSpan.FromMilliseconds(100), $"delivered after {elapsed}");
        Assert.Equal(1, received[0].SequencePosition);
        Assert.Equal(PostgresListenMode.Listening, store.Listener.Mode);
    }

    [Fact]
    public async Task MissedWakeUp_BeforeListenAndWhileDown_IsDeliveredOnCatchUp()
    {
        var logger = new ListLogger<PostgresEventStore>();
        var schema = NewSchema();
        var store = await OpenStoreAsync(schema, logger, options =>
        {
            options.BackupPollInterval = TimeSpan.FromSeconds(30);
            options.PollOnlyInterval = TimeSpan.FromSeconds(30);
        });
        var other = await OpenStoreAsync(schema, initialize: false, configure: options =>
        {
            options.BackupPollInterval = TimeSpan.FromSeconds(30);
            options.PollOnlyInterval = TimeSpan.FromSeconds(30);
        });
        var stream = Stream();

        var delayStart = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Listener.DelayStart = delayStart.Task;
        await using var handle = store.Subscribe("missed", fromSequence: 1);
        await WaitUntilAsync(() => store.Listener.IsDelayingStart);
        await store.AppendAsync(stream, [ContractEvent.Tick()]);
        await NotifyAsync(store.NotifyChannel, "early-hint");
        delayStart.TrySetResult();

        var first = await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(15));
        Assert.Equal(1, first[0].SequencePosition);
        await WaitUntilAsync(() => store.Listener.Mode == PostgresListenMode.Listening && store.Listener.IsWaitingForNotification);

        var holdReconnect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Listener.DelayReconnect = holdReconnect.Task;
        store.Listener.ReconnectDelayOverride = TimeSpan.FromMilliseconds(50);
        await TerminateListenerAsync(store);
        await WaitUntilAsync(() => store.Listener.Mode == PostgresListenMode.Reconnecting);
        await other.AppendAsync(stream, [ContractEvent.Tick()]);
        Assert.False(handle.Events.TryRead(out _));
        holdReconnect.TrySetResult();

        await WaitUntilAsync(() => store.Listener.Mode == PostgresListenMode.Listening, TimeSpan.FromSeconds(20));
        var second = await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(15));
        Assert.Equal(2, second[0].SequencePosition);
        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Warning && entry.Message.Contains("Reconnecting", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DroppedListener_ReconnectsAndDelivers()
    {
        var logger = new ListLogger<PostgresEventStore>();
        var schema = NewSchema();
        var store = await OpenStoreAsync(schema, logger, options =>
        {
            options.BackupPollInterval = TimeSpan.FromSeconds(30);
            options.PollOnlyInterval = TimeSpan.FromSeconds(30);
        });
        var other = await OpenStoreAsync(schema, initialize: false);
        var stream = Stream();
        await store.AppendAsync(stream, [ContractEvent.Tick()]);
        await using var handle = store.Subscribe("dropped", fromSequence: 1);
        var primed = await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(15));
        Assert.Equal(1, primed[0].SequencePosition);
        await WaitUntilAsync(() => store.Listener.IsWaitingForNotification);

        var holdReconnect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Listener.DelayReconnect = holdReconnect.Task;
        store.Listener.ReconnectDelayOverride = TimeSpan.FromMilliseconds(50);
        await TerminateListenerAsync(store);
        await WaitUntilAsync(() => store.Listener.Mode == PostgresListenMode.Reconnecting);
        await other.AppendAsync(stream, [ContractEvent.Tick()]);
        holdReconnect.TrySetResult();
        await WaitUntilAsync(() => store.Listener.Mode == PostgresListenMode.Listening, TimeSpan.FromSeconds(20));
        var received = await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(15));
        Assert.Equal(2, received[0].SequencePosition);
        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Warning && entry.Message.Contains("Reconnecting", StringComparison.Ordinal));
    }

    [Fact]
    public async Task PoolerHost_LogsOneWarning_AndPollOnlyDelivers()
    {
        var logger = new ListLogger<PostgresEventStore>();
        var store = await OpenStoreAsync(NewSchema(), logger, options =>
        {
            options.ListenConnectionString = "Host=db-pooler.example;Port=5432;Username=u;Password=p;Database=lawndart";
            options.PollOnlyInterval = TimeSpan.FromMilliseconds(50);
        });
        var stream = Stream();
        await using var first = store.Subscribe("pool-a", fromSequence: 1);
        await using var second = store.Subscribe("pool-b", fromSequence: 1);
        await WaitUntilAsync(() => store.Listener.Mode == PostgresListenMode.PollOnly);
        await store.AppendAsync(stream, [ContractEvent.Tick(), ContractEvent.Tick()]);
        var received = await ReadExactlyAsync(first, 2, TimeSpan.FromSeconds(10));
        Assert.Equal(new long[] { 1, 2 }, received.Select(e => e.SequencePosition));
        var warnings = logger.Entries.Count(entry =>
            entry.Level == LogLevel.Warning && entry.Message.Contains("Subscriptions are polling.", StringComparison.Ordinal));
        Assert.Equal(1, warnings);
        Assert.Contains(logger.Entries, entry => entry.Message.Contains("transaction pooler", StringComparison.Ordinal));
    }

    [Fact]
    public async Task NotifyOnAppendFalse_SkipsTheWakeUp_AndTheInProcessSignalStillDelivers()
    {
        var store = await OpenStoreAsync(NewSchema(), configure: options =>
        {
            options.NotifyOnAppend = false;
            options.PollOnlyInterval = TimeSpan.FromSeconds(30);
            options.BackupPollInterval = TimeSpan.FromSeconds(30);
        });
        var stream = Stream();
        await using var side = new NpgsqlConnection(_fixture.ConnectionString);
        await side.OpenAsync();
        var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        side.Notification += (_, args) =>
        {
            if (string.Equals(args.Channel, store.NotifyChannel, StringComparison.Ordinal))
                seen.TrySetResult(args.Payload);
        };
        await using (var listen = new NpgsqlCommand($"LISTEN {Quote(store.NotifyChannel)}", side))
            await listen.ExecuteNonQueryAsync();

        await using var handle = store.Subscribe("quiet", fromSequence: 1);
        await WaitUntilAsync(() => store.Listener.Mode == PostgresListenMode.PollOnly);
        await WaitForWaitersAsync(store.NewEvents, 1);
        await store.AppendAsync(stream, [ContractEvent.Tick()]);
        var woke = await side.WaitAsync(200);
        Assert.False(woke);
        Assert.False(seen.Task.IsCompleted);
        var received = await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(5));
        Assert.Equal(1, received[0].SequencePosition);
        Assert.Equal(1, store.NewEvents.Generation);
    }

    [Fact]
    public async Task Append_WakeUpPayload_IsTheNewestPosition()
    {
        var store = await OpenStoreAsync(NewSchema());
        await using var side = new NpgsqlConnection(_fixture.ConnectionString);
        await side.OpenAsync();
        var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        side.Notification += (_, args) =>
        {
            if (string.Equals(args.Channel, store.NotifyChannel, StringComparison.Ordinal))
                seen.TrySetResult(args.Payload);
        };
        await using (var listen = new NpgsqlCommand($"LISTEN {Quote(store.NotifyChannel)}", side))
            await listen.ExecuteNonQueryAsync();

        var result = await store.AppendAsync(Stream(), [ContractEvent.Tick(), ContractEvent.Tick()]);
        var woke = await side.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(woke);
        Assert.Equal(result.SequencePositions[^1].ToString(), await seen.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Listener_UsesListenConnectionString()
    {
        var store = await OpenStoreAsync(NewSchema(), configure: options =>
            options.ListenConnectionString = _fixture.AdminConnectionString);
        await using var handle = store.Subscribe("direct", fromSequence: 1);
        await WaitUntilAsync(() => store.Listener.IsWaitingForNotification);
        await using var connection = new NpgsqlConnection(_fixture.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT usename FROM pg_stat_activity WHERE pid = @pid",
            connection);
        command.Parameters.AddWithValue("pid", store.Listener.BackendPid);
        var user = (string)(await command.ExecuteScalarAsync())!;
        Assert.Equal("postgres", user);
        await store.AppendAsync(Stream(), [ContractEvent.Tick()]);
        var received = await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(10));
        Assert.Equal(1, received[0].SequencePosition);
    }

    [Fact]
    public async Task Burst_TenThousandNotifies_AreDrainedWithoutABusyLoop()
    {
        var store = await OpenStoreAsync(NewSchema(), configure: options =>
        {
            options.BackupPollInterval = TimeSpan.FromSeconds(30);
            options.PollOnlyInterval = TimeSpan.FromSeconds(30);
        });
        await using var handle = store.Subscribe("burst-notify", fromSequence: 1);
        _ = handle;
        await WaitUntilAsync(() => store.Listener.IsWaitingForNotification);
        var before = store.Listener.NotificationCount;
        await using var connection = new NpgsqlConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT pg_notify(@channel, g::text) FROM generate_series(1, 10000) AS g",
            connection);
        command.Parameters.AddWithValue("channel", store.NotifyChannel);
        command.CommandTimeout = 60;
        await command.ExecuteNonQueryAsync();
        await WaitUntilAsync(() => store.Listener.NotificationCount >= before + 10000, TimeSpan.FromSeconds(20));
        await WaitUntilAsync(() => store.Listener.IsWaitingForNotification);
        var wakes = store.Listener.WakeCount;
        Assert.InRange(wakes, 1, 200);
        await WaitUntilAsync(() => store.Listener.IsWaitingForNotification && store.Listener.WakeCount == wakes);
        Assert.Equal(wakes, store.Listener.WakeCount);
    }

    [Fact(Timeout = 240000)]
    public async Task Burst_TenThousandAppends_DeliverAndThenTheListenerWaits()
    {
        var store = await OpenStoreAsync(NewSchema(), configure: options =>
        {
            options.BackupPollInterval = TimeSpan.FromSeconds(30);
            options.SubscriptionChannelCapacity = 20_000;
            options.SubscriptionBatchSize = 1000;
        });
        var stream = Stream();
        await using var handle = store.Subscribe("burst-append", fromSequence: 1);
        await WaitUntilAsync(() => store.Listener.Mode == PostgresListenMode.Listening);
        for (var i = 0; i < 10_000; i++)
            await store.AppendAsync(stream, [ContractEvent.Tick()]);

        var received = await ReadExactlyAsync(handle, 10_000, TimeSpan.FromMinutes(2));
        Assert.Equal(10_000, received.Count);
        Assert.Equal(1, received[0].SequencePosition);
        Assert.Equal(10_000, received[^1].SequencePosition);
        await WaitUntilAsync(() => store.Listener.IsWaitingForNotification);
        var wakes = store.Listener.WakeCount;
        Assert.InRange(wakes, 1, 10_000);
        await WaitUntilAsync(() => store.Listener.IsWaitingForNotification && store.Listener.WakeCount == wakes);
        Assert.Equal(wakes, store.Listener.WakeCount);
    }

    [Fact]
    public async Task Restart_ReconnectsAndDeliversTheRest()
    {
        var hostPort = FreeTcpPort();
        var image = PostgresTestImage.Current;
        await using var container = new PostgreSqlBuilder(image)
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
        var logger = new ListLogger<PostgresEventStore>();
        var store = new PostgresEventStore(
            connectionString,
            options: new PostgresEventStoreOptions
            {
                ConnectionString = connectionString,
                SchemaName = "restarted",
                RequireTenantId = false,
                BackupPollInterval = TimeSpan.FromSeconds(30),
                PollOnlyInterval = TimeSpan.FromSeconds(30)
            },
            logger: logger);
        store.Listener.ReconnectDelayOverride = TimeSpan.FromMilliseconds(50);
        store.Listener.KeepAliveSeconds = 2;
        await store.InitializeSchemaAsync();
        var stream = Stream();
        await using var handle = store.Subscribe("restart", fromSequence: 1);
        await store.AppendAsync(stream, [ContractEvent.Tick()]);
        var first = await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(15));
        Assert.Equal(1, first[0].SequencePosition);
        await WaitUntilAsync(() => store.Listener.IsWaitingForNotification);

        await container.StopAsync();
        await WaitUntilAsync(() => store.Listener.Mode == PostgresListenMode.Reconnecting, TimeSpan.FromSeconds(20));
        await container.StartAsync();
        await WaitUntilAsync(() => store.Listener.Mode == PostgresListenMode.Listening, TimeSpan.FromSeconds(30));
        await store.AppendAsync(stream, [ContractEvent.Tick()]);
        var second = await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(20));
        Assert.Equal(2, second[0].SequencePosition);
        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Warning && entry.Message.Contains("Reconnecting", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Listener_IsNeverIdleInATransaction()
    {
        var store = await OpenStoreAsync(NewSchema());
        await using var handle = store.Subscribe("idle", fromSequence: 1);
        await WaitUntilAsync(() => store.Listener.IsWaitingForNotification);
        var before = await ListenerActivityAsync(store.Listener.BackendPid);
        Assert.Equal("idle", before.State);
        Assert.True(before.TransactionStartIsNull);

        await store.AppendAsync(Stream(), [ContractEvent.Tick()]);
        await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(10));
        await WaitUntilAsync(() => store.Listener.IsWaitingForNotification);
        var after = await ListenerActivityAsync(store.Listener.BackendPid);
        Assert.Equal("idle", after.State);
        Assert.True(after.TransactionStartIsNull);
        Assert.DoesNotContain("transaction", after.State, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task NotificationQueueHealthCheck_ReportsUsage()
    {
        var check = new PostgresNotificationQueueHealthCheck(_fixture.ConnectionString);
        var result = await check.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Healthy, result.Status);
        var reported = Assert.IsType<double>(result.Data["notificationQueueUsage"]);
        await using var connection = new NpgsqlConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT pg_notification_queue_usage()", connection);
        var direct = Convert.ToDouble(await command.ExecuteScalarAsync());
        Assert.Equal(direct, reported);
        Assert.InRange(reported, 0d, 1d);
    }

    private async Task<PostgresEventStore> OpenStoreAsync(
        string schema,
        ListLogger<PostgresEventStore>? logger = null,
        Action<PostgresEventStoreOptions>? configure = null,
        bool initialize = true)
    {
        var options = new PostgresEventStoreOptions
        {
            ConnectionString = _fixture.ConnectionString,
            MigrationConnectionString = _fixture.AdminConnectionString,
            SchemaName = schema,
            ContextName = schema,
            RequireTenantId = false,
            BackupPollInterval = TimeSpan.FromSeconds(30),
            PollOnlyInterval = TimeSpan.FromMilliseconds(50)
        };
        configure?.Invoke(options);
        var store = new PostgresEventStore(_fixture.ConnectionString, serializer: null, options, logger);
        if (initialize)
            await store.InitializeSchemaAsync();
        return store;
    }

    private async Task NotifyAsync(string channel, string payload)
    {
        await using var connection = new NpgsqlConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT pg_notify(@channel, @payload)", connection);
        command.Parameters.AddWithValue("channel", channel);
        command.Parameters.AddWithValue("payload", payload);
        await command.ExecuteNonQueryAsync();
    }

    private async Task TerminateListenerAsync(PostgresEventStore store)
    {
        var pid = store.Listener.BackendPid;
        Assert.True(pid > 0);
        await using var connection = new NpgsqlConnection(_fixture.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT pg_terminate_backend(@pid)", connection);
        command.Parameters.AddWithValue("pid", pid);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<(string State, bool TransactionStartIsNull)> ListenerActivityAsync(int pid)
    {
        await using var connection = new NpgsqlConnection(_fixture.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT state, xact_start IS NULL FROM pg_stat_activity WHERE pid = @pid",
            connection);
        command.Parameters.AddWithValue("pid", pid);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return (reader.GetString(0), reader.GetBoolean(1));
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(15));
        while (!condition())
            await Task.Delay(10, cts.Token);
    }

    private static async Task WaitForWaitersAsync(NewEventsSignal signal, int count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (signal.WaitingCount < count)
            await Task.Delay(10, cts.Token);
    }

    private static async Task<List<SequencedEvent>> ReadExactlyAsync(ISubscriptionHandle handle, int count, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        var list = new List<SequencedEvent>(count);
        while (list.Count < count)
        {
            if (!await handle.Events.WaitToReadAsync(cts.Token))
                throw new TimeoutException($"Channel completed after {list.Count} of {count} events.");
            while (list.Count < count && handle.Events.TryRead(out var sequenced))
                list.Add(sequenced);
        }

        return list;
    }

    private static int FreeTcpPort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static string NewSchema() => "s" + Guid.NewGuid().ToString("N");

    private static string Stream() => "Order:" + Guid.NewGuid().ToString("N");

    private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
