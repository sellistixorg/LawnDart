using System.Diagnostics;
using System.Text;
using LawnDart.EventSourcing.Postgres.Snapshots;
using LawnDart.EventSourcing.SqlServer.Snapshots;
using LawnDart.EventStore;
using LawnDart.Snapshots;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace LawnDart.SqlPerformance;

internal sealed class CaseResult
{
    public string Workload { get; set; } = "";
    public string Name { get; set; } = "";
    public int Count { get; set; }
    public double MeanMilliseconds { get; set; }
    public double P50Milliseconds { get; set; }
    public double P95Milliseconds { get; set; }
    public double P99Milliseconds { get; set; }
    public double ThroughputPerSecond { get; set; }
    public long Events { get; set; }
}

internal sealed class OperationLog
{
    private readonly List<double> _samples = [];
    private readonly object _gate = new();
    private long _events;
    private long _started;

    public void Start() => _started = Stopwatch.GetTimestamp();

    public void Add(long began, long ended, int events)
    {
        var milliseconds = (ended - began) * 1000d / Stopwatch.Frequency;
        lock (_gate)
        {
            _samples.Add(milliseconds);
            _events += events;
        }
    }

    public CaseResult Freeze(string workload, string name, bool throughputFromSamples = false)
    {
        double[] copy;
        long events;
        lock (_gate)
        {
            copy = _samples.ToArray();
            events = _events;
        }

        Array.Sort(copy);
        double wall;
        if (throughputFromSamples)
        {
            wall = copy.Sum() / 1000d;
        }
        else
        {
            wall = _started == 0 ? 0 : (Stopwatch.GetTimestamp() - _started) / (double)Stopwatch.Frequency;
            if (wall <= 0)
                wall = copy.Length == 0 ? 0 : copy.Sum() / 1000d;
        }

        return new CaseResult
        {
            Workload = workload,
            Name = name,
            Count = copy.Length,
            MeanMilliseconds = copy.Length == 0 ? 0 : copy.Average(),
            P50Milliseconds = Percentile(copy, 0.50),
            P95Milliseconds = Percentile(copy, 0.95),
            P99Milliseconds = Percentile(copy, 0.99),
            ThroughputPerSecond = wall <= 0 ? 0 : events / wall,
            Events = events
        };
    }

    private static double Percentile(double[] sorted, double p)
    {
        if (sorted.Length == 0)
            return 0;
        var rank = (sorted.Length - 1) * p;
        var low = (int)Math.Floor(rank);
        var high = (int)Math.Ceiling(rank);
        if (low == high)
            return sorted[low];
        return sorted[low] + ((sorted[high] - sorted[low]) * (rank - low));
    }
}

internal static class Workloads
{
    private static readonly byte[] Payload = PayloadFactory.Make("order-placed", 1, 400);
    private static readonly int[] Writers = [1, 8, 32, 128];

    public static async Task<List<CaseResult>> RunAsync(
        EngineEndpoint endpoint,
        DatasetCatalog catalog,
        int samples,
        bool fullCatchUp,
        CancellationToken cancellationToken)
    {
        if (endpoint.Kind == DatabaseKind.Postgres)
        {
            var store = await StoreFactory.OpenPgAsync(endpoint.DatabaseConnectionString, cancellationToken).ConfigureAwait(false);
            var snapshots = new PostgresSnapshotStore(StoreFactory.PgOptions(endpoint.DatabaseConnectionString));
            return await RunOnStoreAsync(endpoint, store, snapshots, catalog, samples, fullCatchUp, cancellationToken).ConfigureAwait(false);
        }

        var sql = await StoreFactory.OpenSqlAsync(endpoint.DatabaseConnectionString, cancellationToken).ConfigureAwait(false);
        var sqlSnapshots = new SqlServerSnapshotStore(StoreFactory.SqlOptions(endpoint.DatabaseConnectionString));
        return await RunOnStoreAsync(endpoint, sql, sqlSnapshots, catalog, samples, fullCatchUp, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<List<CaseResult>> RunOnStoreAsync(
        EngineEndpoint endpoint,
        IEventLog log,
        ISnapshotStore snapshots,
        DatasetCatalog catalog,
        int samples,
        bool fullCatchUp,
        CancellationToken cancellationToken)
    {
        if (log is not IEventLogSubscriptions subscriptions)
            throw new InvalidOperationException("The store does not support subscriptions.");

        var results = new List<CaseResult>();
        var streams = catalog.Streams.Where(stream => stream.Id != catalog.HotStreamId).ToList();
        var loadStreams = streams.Where(stream => stream.SnapshotVersion > 0 && stream.SnapshotVersion < stream.Version).ToList();
        if (loadStreams.Count < 1 || streams.Count < Writers[^1])
            throw new InvalidOperationException("Not enough loaded streams for the append and read workloads.");
        results.AddRange(await StreamLoadsAsync(log, snapshots, endpoint, loadStreams, samples, cancellationToken).ConfigureAwait(false));
        results.AddRange(await AppendStreamsAsync(log, streams, samples, cancellationToken).ConfigureAwait(false));
        results.Add(await HotStreamAsync(log, catalog, samples, cancellationToken).ConfigureAwait(false));
        results.Add(await DcbAppendsAsync(log, catalog, samples, recentAfter: true, "dcb append recent after", cancellationToken).ConfigureAwait(false));
        results.Add(await QueryAsync(log, Query.FromItems(QueryItem.ByTags(catalog.QueryTag)), samples, "queries", "tag", cancellationToken).ConfigureAwait(false));
        results.Add(await QueryAsync(log, Query.FromItems(QueryItem.ByTypeAndTags([catalog.QueryType], [catalog.QueryTag])), samples, "queries", "tag and type", cancellationToken).ConfigureAwait(false));
        results.Add(await QueryAsync(log, Query.FromItems(QueryItem.ByTags(catalog.MultiTagA, catalog.MultiTagB)), samples, "queries", "two tags", cancellationToken).ConfigureAwait(false));
        results.Add(await ConditionReadAsync(log, catalog, samples, recentAfter: true, "condition recent after", cancellationToken).ConfigureAwait(false));
        results.Add(await ConditionReadAsync(log, catalog, samples, recentAfter: false, "condition empty after", cancellationToken).ConfigureAwait(false));
        results.Add(await CatchUpAsync(log, 1000, fullCatchUp ? int.MaxValue : 2, cancellationToken).ConfigureAwait(false));
        results.Add(await CatchUpAsync(log, 5000, fullCatchUp ? int.MaxValue : 2, cancellationToken).ConfigureAwait(false));
        var trials = fullCatchUp ? 5 : 1;
        results.Add(await PollAsync(endpoint, log, subscriptions, trials, cancellationToken).ConfigureAwait(false));
        return results;
    }

    private static async Task<List<CaseResult>> AppendStreamsAsync(
        IEventLog log,
        IReadOnlyList<StreamSample> streams,
        int samples,
        CancellationToken cancellationToken)
    {
        var results = new List<CaseResult>();
        var versions = new Dictionary<string, long>(StringComparer.Ordinal);
        var cursor = 0;
        // Cold stream loads clear the buffer pool. The first appends then pay
        // for plan compilation and for the pages the append path keeps hot.
        // Write those before the clock starts.
        versions[streams[0].Id] = await WarmAppendPathAsync(log, streams[0], cancellationToken).ConfigureAwait(false);
        foreach (var writers in Writers)
        {
            if (streams.Count < writers)
                throw new InvalidOperationException("Not enough loaded streams for the append writers.");

            var start = cursor;
            if (start + writers > streams.Count)
                start = 0;
            var rounds = Math.Max(1, samples / writers);
            var operations = new OperationLog();
            operations.Start();
            var tasks = new Task[writers];
            for (var writer = 0; writer < writers; writer++)
            {
                var sample = streams[start + writer];
                var version = versions.TryGetValue(sample.Id, out var seen) ? seen : sample.Version;
                versions[sample.Id] = version + rounds;
                tasks[writer] = AppendWriterAsync(log, sample, version, rounds, operations, cancellationToken);
            }

            cursor = start + writers;

            await Task.WhenAll(tasks).ConfigureAwait(false);
            results.Add(operations.Freeze("appends", "stream writers " + writers.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        }

        return results;
    }

    private static async Task<long> WarmAppendPathAsync(
        IEventLog log,
        StreamSample sample,
        CancellationToken cancellationToken)
    {
        var version = sample.Version;
        var metadata = Encoding.UTF8.GetBytes("{\"TenantId\":\"" + TenantOf(sample.Id) + "\"}");
        for (var i = 0; i < 32; i++)
        {
            var result = await log.AppendAsync(
                sample.Id,
                [new AppendEvent("order-placed", Payload, metadata, tags: sample.Tags)],
                version,
                cancellationToken).ConfigureAwait(false);
            if (result.SequencePositions.Count < 1)
                throw new InvalidOperationException("A warm-up append returned no position.");
            version++;
        }

        return version;
    }

    private static async Task AppendWriterAsync(
        IEventLog log,
        StreamSample sample,
        long version,
        int rounds,
        OperationLog operations,
        CancellationToken cancellationToken)
    {
        var metadata = Encoding.UTF8.GetBytes("{\"TenantId\":\"" + TenantOf(sample.Id) + "\"}");
        for (var round = 0; round < rounds; round++)
        {
            var began = Stopwatch.GetTimestamp();
            var result = await log.AppendAsync(
                sample.Id,
                [new AppendEvent("order-placed", Payload, metadata, tags: sample.Tags)],
                version,
                cancellationToken).ConfigureAwait(false);
            operations.Add(began, Stopwatch.GetTimestamp(), 1);
            if (result.SequencePositions.Count < 1)
                throw new InvalidOperationException("A stream append returned no position.");
            version++;
        }
    }

    private static async Task<CaseResult> HotStreamAsync(
        IEventLog log,
        DatasetCatalog catalog,
        int samples,
        CancellationToken cancellationToken)
    {
        var operations = new OperationLog();
        operations.Start();
        var version = catalog.HotVersion;
        var metadata = Encoding.UTF8.GetBytes("{\"TenantId\":\"" + TenantOf(catalog.HotStreamId) + "\"}");
        var rounds = Math.Max(1, samples);
        for (var round = 0; round < rounds; round++)
        {
            var began = Stopwatch.GetTimestamp();
            var result = await log.AppendAsync(
                catalog.HotStreamId,
                [new AppendEvent("order-shipped", Payload, metadata, tags: catalog.HotTags)],
                version,
                cancellationToken).ConfigureAwait(false);
            operations.Add(began, Stopwatch.GetTimestamp(), 1);
            if (result.SequencePositions.Count < 1)
                throw new InvalidOperationException("The hot stream append returned no position.");
            version++;
        }

        return operations.Freeze("appends", "hot stream");
    }

    private static async Task<CaseResult> DcbAppendsAsync(
        IEventLog log,
        DatasetCatalog catalog,
        int samples,
        bool recentAfter,
        string name,
        CancellationToken cancellationToken)
    {
        var operations = new OperationLog();
        operations.Start();
        var rounds = Math.Max(1, samples);
        for (var round = 0; round < rounds; round++)
        {
            var tenant = "t" + (round % Math.Max(1, catalog.Tenants)).ToString(System.Globalization.CultureInfo.InvariantCulture);
            var metadata = Encoding.UTF8.GetBytes("{\"TenantId\":\"" + tenant + "\"}");
            var condition = AppendCondition.FailIfMatches(
                Query.FromItems(QueryItem.ByTags("bench-absent")),
                recentAfter ? catalog.MaxPosition : null);
            var began = Stopwatch.GetTimestamp();
            var result = await log.AppendAsync(
                [new AppendEvent("note-added", Payload, metadata, tags: ["tenant:" + tenant])],
                condition,
                cancellationToken).ConfigureAwait(false);
            operations.Add(began, Stopwatch.GetTimestamp(), 1);
            if (result.SequencePositions.Count < 1)
                throw new InvalidOperationException("A conditional append returned no position.");
        }

        return operations.Freeze("appends", name);
    }

    private static async Task<CaseResult> ConditionReadAsync(
        IEventLog log,
        DatasetCatalog catalog,
        int samples,
        bool recentAfter,
        string name,
        CancellationToken cancellationToken)
    {
        var operations = new OperationLog();
        operations.Start();
        var query = Query.FromItems(QueryItem.ByTags("bench-absent"));
        // The append condition ignores positions at or before After. This read is inclusive, so start one past the loaded head.
        long? from = recentAfter ? catalog.MaxPosition + 1 : null;
        var rounds = Math.Max(1, samples);
        for (var round = 0; round < rounds; round++)
        {
            var began = Stopwatch.GetTimestamp();
            var position = await log.GetMaxSequencePositionAsync(query, from, cancellationToken: cancellationToken).ConfigureAwait(false);
            operations.Add(began, Stopwatch.GetTimestamp(), 1);
            if (position != 0)
                throw new InvalidOperationException("The condition query matched a stored event.");
        }

        return operations.Freeze("queries", name);
    }

    private static async Task<CaseResult> QueryAsync(
        IEventLog log,
        Query query,
        int samples,
        string workload,
        string name,
        CancellationToken cancellationToken)
    {
        var operations = new OperationLog();
        operations.Start();
        var rounds = Math.Max(1, samples);
        for (var round = 0; round < rounds; round++)
        {
            var began = Stopwatch.GetTimestamp();
            var page = await log.ReadByQueryAsync(query, limit: 50, cancellationToken: cancellationToken).ConfigureAwait(false);
            operations.Add(began, Stopwatch.GetTimestamp(), page.Events.Count);
            if (!page.TypesResolved)
                throw new InvalidOperationException("A query type was not stored.");
        }

        return operations.Freeze(workload, name);
    }

    private static async Task<List<CaseResult>> StreamLoadsAsync(
        IEventLog log,
        ISnapshotStore snapshots,
        EngineEndpoint endpoint,
        IReadOnlyList<StreamSample> streams,
        int samples,
        CancellationToken cancellationToken)
    {
        if (streams.Count < 1)
            throw new InvalidOperationException("Not enough loaded streams to read.");

        var flush = await ChooseFlushAsync(endpoint, cancellationToken).ConfigureAwait(false);
        if (endpoint.Kind == DatabaseKind.Postgres)
        {
            Console.WriteLine(flush == CacheFlush.ConnectionOnly
                ? "postgres stream load: connection-cold"
                : "postgres stream load: cold");
        }

        var coldName = flush == CacheFlush.ConnectionOnly ? "connection-cold" : "cold";
        var coldRounds = Math.Max(1, Math.Min(samples, 20));
        var cold = new OperationLog();
        cold.Start();
        for (var round = 0; round < coldRounds; round++)
        {
            var sample = streams[round % streams.Count];
            await ApplyFlushAsync(endpoint, flush, cancellationToken).ConfigureAwait(false);
            var began = Stopwatch.GetTimestamp();
            var events = await LoadTailAsync(log, snapshots, sample, cancellationToken).ConfigureAwait(false);
            cold.Add(began, Stopwatch.GetTimestamp(), events);
        }

        var warm = new OperationLog();
        warm.Start();
        var again = streams[0];
        var warmRounds = Math.Max(1, samples);
        for (var round = 0; round < warmRounds; round++)
        {
            var began = Stopwatch.GetTimestamp();
            var events = await LoadTailAsync(log, snapshots, again, cancellationToken).ConfigureAwait(false);
            warm.Add(began, Stopwatch.GetTimestamp(), events);
        }

        return
        [
            cold.Freeze("stream loads", coldName),
            warm.Freeze("stream loads", "warm")
        ];
    }

    private static async Task<int> LoadTailAsync(
        IEventLog log,
        ISnapshotStore snapshots,
        StreamSample sample,
        CancellationToken cancellationToken)
    {
        var loaded = await snapshots.LoadSnapshotAsync<BenchState>(sample.Id, cancellationToken).ConfigureAwait(false);
        if (loaded.Info is null || loaded.Info.Version != sample.SnapshotVersion)
            throw new InvalidOperationException("A snapshot load did not return the stored version.");
        var tail = await log.ReadStreamAsync(sample.Id, sample.SnapshotVersion + 1, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (tail.Count != sample.Version - sample.SnapshotVersion)
            throw new InvalidOperationException("A stream tail length changed.");
        return tail.Count + 1;
    }

    private static async Task<CaseResult> CatchUpAsync(
        IEventLog log,
        int batchSize,
        int maxBatches,
        CancellationToken cancellationToken)
    {
        var operations = new OperationLog();
        operations.Start();
        long? from = null;
        var batches = 0;
        while (batches < maxBatches)
        {
            var began = Stopwatch.GetTimestamp();
            var page = await log.ReadByQueryAsync(Query.All(), from, batchSize, cancellationToken: cancellationToken).ConfigureAwait(false);
            operations.Add(began, Stopwatch.GetTimestamp(), page.Events.Count);
            batches++;
            if (page.Events.Count == 0)
                break;
            from = page.Events[^1].SequencePosition + 1;
            if (page.Events.Count < batchSize)
                break;
        }

        if (operations.Freeze("catch-up", "batch " + batchSize.ToString(System.Globalization.CultureInfo.InvariantCulture)).Events < 1)
            throw new InvalidOperationException("Catch-up read no events.");
        return operations.Freeze("catch-up", "batch " + batchSize.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    private static async Task<CaseResult> PollAsync(
        EngineEndpoint endpoint,
        IEventLog log,
        IEventLogSubscriptions subscriptions,
        int trials,
        CancellationToken cancellationToken)
    {
        var operations = new OperationLog();
        for (var trial = 0; trial < trials; trial++)
        {
            var head = await log.GetCurrentSequenceAsync(cancellationToken).ConfigureAwait(false);
            await using var handle = subscriptions.Subscribe("sqlperf-poll-" + trial.ToString(System.Globalization.CultureInfo.InvariantCulture), head + 1, cancellationToken: cancellationToken);
            await using var writer = await OpenWriterAsync(endpoint, cancellationToken).ConfigureAwait(false);
            var metadata = Encoding.UTF8.GetBytes("{\"TenantId\":\"t0\"}");
            var appended = await log.AppendAsync(
                [new AppendEvent("note-added", Payload, metadata, tags: ["tenant:t0"])],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags("bench-absent")), head),
                cancellationToken).ConfigureAwait(false);
            if (appended.SequencePositions.Count < 1)
                throw new InvalidOperationException("The polling append returned no position.");

            using (var duringHold = new CancellationTokenSource(TimeSpan.FromMilliseconds(400)))
            using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, duringHold.Token))
            {
                try
                {
                    await handle.Events.ReadAsync(linked.Token).ConfigureAwait(false);
                    throw new InvalidOperationException("A subscription delivered an event while a writer was still open.");
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    // The open writer still hides the event.
                }
            }

            await writer.DisposeAsync().ConfigureAwait(false);
            // The open writer and the append sit outside this sample.
            var began = Stopwatch.GetTimestamp();
            using var wait = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            using var linkedWait = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, wait.Token);
            try
            {
                var delivered = await handle.Events.ReadAsync(linkedWait.Token).ConfigureAwait(false);
                operations.Add(began, Stopwatch.GetTimestamp(), 1);
                if (delivered.SequencePosition < 1)
                    throw new InvalidOperationException("The subscription delivered an empty position.");
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw new InvalidOperationException("The subscription did not deliver the appended event.");
            }
        }

        // Throughput is the wait after commit, not the hold and not the append.
        return operations.Freeze("catch-up", "subscription poll", throughputFromSamples: true);
    }

    private enum CacheFlush
    {
        SqlBuffers,
        PostgresBuffers,
        PostgresRestart,
        ConnectionOnly
    }

    private static async Task<CacheFlush> ChooseFlushAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        if (endpoint.Kind == DatabaseKind.SqlServer)
            return CacheFlush.SqlBuffers;

        await using var connection = new NpgsqlConnection(endpoint.DatabaseConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await ExecuteNpgsqlAsync(connection, "CHECKPOINT", cancellationToken).ConfigureAwait(false);
        if (await TryEvictSharedBuffersAsync(connection, cancellationToken).ConfigureAwait(false))
            return CacheFlush.PostgresBuffers;
        if (endpoint.OwnedContainer && !string.IsNullOrEmpty(endpoint.ContainerName))
            return CacheFlush.PostgresRestart;
        return CacheFlush.ConnectionOnly;
    }

    private static async Task ApplyFlushAsync(EngineEndpoint endpoint, CacheFlush flush, CancellationToken cancellationToken)
    {
        switch (flush)
        {
            case CacheFlush.SqlBuffers:
            {
                await using var connection = new SqlConnection(endpoint.DatabaseConnectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                await using var command = new SqlCommand("CHECKPOINT; DBCC DROPCLEANBUFFERS;", connection) { CommandTimeout = 0 };
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            case CacheFlush.PostgresBuffers:
            {
                await using var connection = new NpgsqlConnection(endpoint.DatabaseConnectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                await ExecuteNpgsqlAsync(connection, "CHECKPOINT", cancellationToken).ConfigureAwait(false);
                if (!await TryEvictSharedBuffersAsync(connection, cancellationToken).ConfigureAwait(false))
                    throw new InvalidOperationException("Postgres could not evict shared buffers.");
                return;
            }
            case CacheFlush.PostgresRestart:
            {
                await using (var connection = new NpgsqlConnection(endpoint.DatabaseConnectionString))
                {
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    await ExecuteNpgsqlAsync(connection, "CHECKPOINT", cancellationToken).ConfigureAwait(false);
                }

                await Engines.RestartOwnedAsync(endpoint, cancellationToken).ConfigureAwait(false);
                return;
            }
            default:
            {
                await using var connection = new NpgsqlConnection(endpoint.DatabaseConnectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                await ExecuteNpgsqlAsync(connection, "DISCARD ALL", cancellationToken).ConfigureAwait(false);
                NpgsqlConnection.ClearAllPools();
                return;
            }
        }
    }

    private static async Task<bool> TryEvictSharedBuffersAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        try
        {
            await ExecuteNpgsqlAsync(connection, "CREATE EXTENSION IF NOT EXISTS pg_buffercache", cancellationToken).ConfigureAwait(false);
            await ExecuteNpgsqlAsync(connection, "SELECT buffers_evicted FROM public.pg_buffercache_evict_all()", cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (PostgresException ex) when (IsUnavailableColdCache(ex))
        {
            return false;
        }
    }

    private static bool IsUnavailableColdCache(PostgresException exception)
        => exception.SqlState is "42883" or "42501" or "0A000" or "58P01" or "3F000";

    private static async Task ExecuteNpgsqlAsync(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 0 };
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<IAsyncDisposable> OpenWriterAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        if (endpoint.Kind == DatabaseKind.Postgres)
        {
            var connection = new NpgsqlConnection(endpoint.DatabaseConnectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using (var current = new NpgsqlCommand("SELECT pg_current_xact_id()", connection, transaction))
                await current.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            await using (var next = new NpgsqlCommand("SELECT nextval('lawndart.event_position')", connection, transaction))
                await next.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return new HeldConnection(async () =>
            {
                await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                await transaction.DisposeAsync().ConfigureAwait(false);
                await connection.DisposeAsync().ConfigureAwait(false);
            });
        }

        var sql = new SqlConnection(endpoint.DatabaseConnectionString);
        await sql.OpenAsync(cancellationToken).ConfigureAwait(false);
        var sqlTransaction = (SqlTransaction)await sql.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using (var insert = new SqlCommand("INSERT INTO dbo.AppendFence DEFAULT VALUES", sql, sqlTransaction))
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        return new HeldConnection(async () =>
        {
            await sqlTransaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            await sqlTransaction.DisposeAsync().ConfigureAwait(false);
            await sql.DisposeAsync().ConfigureAwait(false);
        });
    }

    private static string TenantOf(string streamId)
    {
        var colon = streamId.IndexOf(':');
        return colon > 0 ? streamId[..colon] : "t0";
    }

    private sealed class HeldConnection(Func<ValueTask> release) : IAsyncDisposable
    {
        private int _released;

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _released, 1) != 0)
                return ValueTask.CompletedTask;
            return release();
        }
    }
}
