using System.Diagnostics;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// One-shot append rate. Not part of the pull request suite.
/// </summary>
/// <remarks>
/// Testcontainers turns Postgres durability off (<c>fsync=off</c>,
/// <c>synchronous_commit=off</c>, <c>full_page_writes=off</c>). This benchmark
/// turns those three back on so the comparison with SQL Server is durable
/// on both sides. Each window is 30 seconds after a warmup.
/// The earlier run on this machine, with durability off and 4 second windows, was:
/// single event Postgres 786 and SQL Server 360 appends per second;
/// 10-event batch Postgres 655 and SQL Server 180 appends per second.
/// </remarks>
[Trait("Category", "Benchmark")]
public sealed class PostgresBenchmarkTests
{
    /// <summary>Durability settings this benchmark forces on the Postgres container.</summary>
    public const string PostgresDurabilityCommand =
        "fsync=on, synchronous_commit=on, full_page_writes=on";

    [Fact]
    public async Task AppendsPerSecond_SingleAndBatchOfTen()
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
        var sql = new MsSqlBuilder(MsSqlTestImage.Server2025)
            .WithPassword("Test123!")
            .Build();
        await Task.WhenAll(postgres.StartAsync(), sql.StartAsync());
        try
        {
            var pgStore = new PostgresEventStore(postgres.GetConnectionString(), options: new PostgresEventStoreOptions
            {
                ConnectionString = postgres.GetConnectionString(),
                SchemaName = "bench",
                RequireTenantId = false
            });
            await pgStore.InitializeSchemaAsync();
            await AssertPostgresDurabilityOnAsync(postgres.GetConnectionString());

            var sqlOptions = new SqlServerEventStoreOptions
            {
                ConnectionString = sql.GetConnectionString(),
                SchemaName = "bench",
                RequireTenantId = false,
                ContextName = "bench"
            };
            var sqlStore = new SqlServerEventStore(sql.GetConnectionString(), options: sqlOptions);
            await sqlStore.InitializeSchemaAsync();

            var window = TimeSpan.FromSeconds(30);
            var singlePg = await MeasureAsync(pgStore, 1, window);
            var batchPg = await MeasureAsync(pgStore, 10, window);
            var singleSql = await MeasureAsync(sqlStore, 1, window);
            var batchSql = await MeasureAsync(sqlStore, 10, window);

            var text = $"""
                durable settings: {PostgresDurabilityCommand}
                window: 30 seconds after warmup
                single-event Postgres {singlePg:F0} appends/s
                single-event SQL Server {singleSql:F0} appends/s
                10-event Postgres {batchPg:F0} appends/s
                10-event SQL Server {batchSql:F0} appends/s
                previous run with Testcontainers durability off and 4 second windows:
                single-event Postgres 786 appends/s, SQL Server 360 appends/s
                10-event Postgres 655 appends/s, SQL Server 180 appends/s
                """;
            Directory.CreateDirectory("/tmp/lawndart-bench");
            await File.WriteAllTextAsync("/tmp/lawndart-bench/result.txt", text);
            Assert.True(singlePg > 0 && batchPg > 0 && singleSql > 0 && batchSql > 0, text);
        }
        finally
        {
            await postgres.DisposeAsync();
            await sql.DisposeAsync();
        }
    }

    private static async Task<double> MeasureAsync(IEventStore store, int batch, TimeSpan window)
    {
        static IEvent[] Fresh(int count)
            => Enumerable.Range(0, count).Select(_ => (IEvent)ContractEvent.Tick()).ToArray();

        var stream = "bench:Order:" + Guid.NewGuid().ToString("N");
        var warmupUntil = Stopwatch.StartNew();
        var warmup = 0;
        while (warmupUntil.Elapsed < TimeSpan.FromSeconds(5))
        {
            await store.AppendAsync(stream + "w" + warmup, Fresh(batch));
            warmup++;
        }

        var started = Stopwatch.StartNew();
        var appends = 0;
        while (started.Elapsed < window)
        {
            await store.AppendAsync(stream + "m" + appends, Fresh(batch));
            appends++;
        }

        return appends / started.Elapsed.TotalSeconds;
    }

    private static async Task AssertPostgresDurabilityOnAsync(string connectionString)
    {
        await using var connection = new Npgsql.NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new Npgsql.NpgsqlCommand(
            "SELECT current_setting('fsync'), current_setting('synchronous_commit'), current_setting('full_page_writes')",
            connection);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("on", reader.GetString(0));
        Assert.Equal("on", reader.GetString(1));
        Assert.Equal("on", reader.GetString(2));
    }
}
