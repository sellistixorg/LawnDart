using System.Diagnostics;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventStore;
using LawnDart.TestUtilities;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit.Abstractions;

namespace LawnDart.Postgres.Benchmarks;

/// <summary>
/// Counts appends, wake-up delivery, and hash buckets.
/// Tagged <c>Category=Benchmark</c> so the pull request test run skips it.
/// The container starts with fsync, synchronous_commit, and full_page_writes on.
/// </summary>
[Trait("Category", "Benchmark")]
public sealed class PostgresStoreBenchmarks(ITestOutputHelper output)
{
    private static readonly TimeSpan Warmup = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan PartitionWarmup = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan PartitionWindow = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task Appends_WakeUp_SubscriptionDelay_AndPartitionCounts()
    {
        var postgres = new PostgreSqlBuilder(PostgresTestImage.Version18)
            .WithDatabase("lawndart")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .WithCommand(
                "-c", "fsync=on",
                "-c", "synchronous_commit=on",
                "-c", "full_page_writes=on")
            .Build();
        await postgres.StartAsync();
        try
        {
            var connectionString = postgres.GetConnectionString();

            await RequireAppendsAsync(connectionString, "append1", partitionCount: 8, notify: true, batchSize: 1, Warmup, Window, "single event, 8 buckets");
            await RequireAppendsAsync(connectionString, "append10", partitionCount: 8, notify: true, batchSize: 10, Warmup, Window, "10 events, 8 buckets");

            await RequireAppendsAsync(connectionString, "notifyon", partitionCount: 8, notify: true, batchSize: 1, Warmup, Window, "wake-up on");
            await RequireAppendsAsync(connectionString, "notifyoff", partitionCount: 8, notify: false, batchSize: 1, Warmup, Window, "wake-up off");

            await RequireSubscriptionDelayAsync(connectionString);

            await RequirePartitionCountAsync(connectionString, "part1", 1);
            await RequirePartitionCountAsync(connectionString, "part8", 8);
            await RequirePartitionCountAsync(connectionString, "part64", 64);
        }
        finally
        {
            await postgres.DisposeAsync();
        }
    }

    private async Task RequirePartitionCountAsync(string connectionString, string schema, int partitionCount)
    {
        var store = await OpenAsync(connectionString, schema, partitionCount, notify: true);
        var buckets = await CountHashBucketsAsync(connectionString, schema);
        Assert.Equal(partitionCount, buckets);
        await RequireAppendsOnStoreAsync(store, batchSize: 1, PartitionWarmup, PartitionWindow, $"{partitionCount} buckets");
    }

    private async Task RequireAppendsAsync(
        string connectionString,
        string schema,
        int partitionCount,
        bool notify,
        int batchSize,
        TimeSpan warmup,
        TimeSpan window,
        string label)
    {
        var store = await OpenAsync(connectionString, schema, partitionCount, notify);
        await RequireAppendsOnStoreAsync(store, batchSize, warmup, window, label);
    }

    private async Task RequireAppendsOnStoreAsync(
        PostgresEventStore store,
        int batchSize,
        TimeSpan warmup,
        TimeSpan window,
        string label)
    {
        var payload = "{}"u8.ToArray();
        AppendEvent[] Fresh()
            => Enumerable.Range(0, batchSize)
                .Select(_ => new AppendEvent("bench.tick", payload))
                .ToArray();

        var warmupClock = Stopwatch.StartNew();
        while (warmupClock.Elapsed < warmup)
            await store.AppendAsync(StreamId(), Fresh());

        var clock = Stopwatch.StartNew();
        long count = 0;
        while (clock.Elapsed < window)
        {
            await store.AppendAsync(StreamId(), Fresh());
            count++;
        }

        var rate = count / clock.Elapsed.TotalSeconds;
        Assert.True(count > 0 && double.IsFinite(rate) && rate > 0);
        output.WriteLine($"{label}: {rate:F1} appends per second");
    }

    private async Task RequireSubscriptionDelayAsync(string connectionString)
    {
        var store = await OpenAsync(connectionString, "subdelay", partitionCount: 8, notify: true);
        var payload = "{}"u8.ToArray();
        await using var handle = ((IEventLogSubscriptions)store).Subscribe("bench-sub", fromSequence: 1);

        await store.AppendAsync(StreamId(), [new AppendEvent("bench.tick", payload)]);
        using (var warmupWait = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
            await handle.Events.ReadAsync(warmupWait.Token);

        long ticks = 0;
        const int samples = 20;
        for (var i = 0; i < samples; i++)
        {
            var started = Stopwatch.GetTimestamp();
            await store.AppendAsync(StreamId(), [new AppendEvent("bench.tick", payload)]);
            using var wait = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var delivered = await handle.Events.ReadAsync(wait.Token);
            ticks += Stopwatch.GetTimestamp() - started;
            Assert.True(delivered.SequencePosition > 0);
        }

        Assert.True(ticks > 0);
        var averageMs = ticks / (double)Stopwatch.Frequency / samples * 1000d;
        output.WriteLine($"subscription delay: {averageMs:F1} milliseconds average over {samples} samples");
    }

    private static async Task<PostgresEventStore> OpenAsync(
        string connectionString,
        string schema,
        int partitionCount,
        bool notify)
    {
        var store = new PostgresEventStore(connectionString, options: new PostgresEventStoreOptions
        {
            ConnectionString = connectionString,
            SchemaName = schema,
            ContextName = schema,
            RequireTenantId = false,
            PartitionCount = partitionCount,
            NotifyOnAppend = notify,
            BackupPollInterval = TimeSpan.FromSeconds(30)
        });
        await store.InitializeSchemaAsync();
        return store;
    }

    private static async Task<int> CountHashBucketsAsync(string connectionString, string schema)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT count(*)::int
            FROM pg_catalog.pg_inherits AS i
            JOIN pg_catalog.pg_class AS parent ON parent.oid = i.inhparent
            JOIN pg_catalog.pg_namespace AS ns ON ns.oid = parent.relnamespace
            WHERE ns.nspname = @schema AND parent.relname = 'events_shared'
            """,
            connection);
        command.Parameters.AddWithValue("schema", schema);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    private static string StreamId() => "bench:Order:" + Guid.NewGuid().ToString("N");
}
