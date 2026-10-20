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
[Trait("Category", "Benchmark")]
public sealed class PostgresBenchmarkTests
{
    [Fact]
    public async Task AppendsPerSecond_SingleAndBatchOfTen()
    {
        var postgres = new PostgreSqlBuilder(PostgresTestImage.Version18)
            .WithDatabase("lawndart")
            .WithUsername("postgres")
            .WithPassword("postgres")
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

            var sqlOptions = new SqlServerEventStoreOptions
            {
                ConnectionString = sql.GetConnectionString(),
                SchemaName = "bench",
                RequireTenantId = false,
                ContextName = "bench"
            };
            var sqlStore = new SqlServerEventStore(sql.GetConnectionString(), options: sqlOptions);
            await sqlStore.InitializeSchemaAsync();

            var singlePg = await MeasureAsync(pgStore, 1, TimeSpan.FromSeconds(4));
            var batchPg = await MeasureAsync(pgStore, 10, TimeSpan.FromSeconds(4));
            var singleSql = await MeasureAsync(sqlStore, 1, TimeSpan.FromSeconds(4));
            var batchSql = await MeasureAsync(sqlStore, 10, TimeSpan.FromSeconds(4));

            var text = $"""
                single-event Postgres {singlePg:F0} appends/s
                single-event SQL Server {singleSql:F0} appends/s
                10-event Postgres {batchPg:F0} appends/s
                10-event SQL Server {batchSql:F0} appends/s
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
        for (var i = 0; i < 30; i++)
            await store.AppendAsync(stream + "w" + i, Fresh(batch));

        var started = Stopwatch.StartNew();
        var appends = 0;
        while (started.Elapsed < window)
        {
            await store.AppendAsync(stream + "m" + appends, Fresh(batch));
            appends++;
        }

        return appends / started.Elapsed.TotalSeconds;
    }
}
