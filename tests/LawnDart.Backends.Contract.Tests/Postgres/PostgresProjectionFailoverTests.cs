using DotNet.Testcontainers.Builders;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Postgres;
using Npgsql;
using Testcontainers.PostgreSql;

namespace LawnDart.Backends.Contract.Tests;

[Trait("Category", "Integration")]
public sealed class PostgresProjectionRestartTests
{
    [Fact(Timeout = 180000)]
    public async Task DatabaseRestart_NeverHasTwoLeaders_AndRejectsTheStaleEpoch()
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
            SchemaName = "restarted",
            LockProbeInterval = TimeSpan.FromMilliseconds(100),
            LeaseDuration = TimeSpan.FromSeconds(1),
            LeaseRenewInterval = TimeSpan.FromMilliseconds(200)
        };
        var views = new PostgresViewStore(options);
        var leaderStore = new PostgresCheckpointStore(options, views);
        var waiterStore = new PostgresCheckpointStore(options, views);
        await views.InitializeSchemaAsync();
        await leaderStore.InitializeSchemaAsync();
        var leaderLock = new PostgresProjectionRunnerLock(leaderStore, options);
        var waiterLock = new PostgresProjectionRunnerLock(waiterStore, options);
        var leader = await leaderLock.AcquireAsync("OrderSummary:v1", 0);
        var pending = waiterLock.AcquireAsync("OrderSummary:v1", 0);
        var both = 0;

        await container.StopAsync();
        await WaitUntilAsync(() => !leader.IsHeld, TimeSpan.FromSeconds(20));
        using var watch = new CancellationTokenSource();
        var sampler = Task.Run(async () =>
        {
            while (!watch.IsCancellationRequested)
            {
                if (leader.IsHeld && pending.IsCompletedSuccessfully && pending.Result.IsHeld)
                    Interlocked.Increment(ref both);
                await Task.Delay(15);
            }
        }, watch.Token);

        await container.StartAsync();
        PostgresProjectionConnections.ClearIdle(connectionString);
        var taken = await pending.WaitAsync(TimeSpan.FromSeconds(40));
        try
        {
            await WaitUntilAsync(() => taken.IsHeld && !leader.IsHeld, TimeSpan.FromSeconds(20));
            var rejected = await Assert.ThrowsAsync<ProjectionLeadershipLostException>(() =>
                leaderStore.SaveCheckpointAsync(new LawnDart.Projections.ProjectionCheckpoint
                {
                    ProjectionType = "OrderSummary:v1",
                    NodeId = 0,
                    LastSequencePosition = 8,
                    LastUpdated = DateTime.UtcNow,
                    TotalEventsProcessed = 8
                }));
            Assert.Contains("was not saved", rejected.Message, StringComparison.Ordinal);
            await waiterStore.SaveCheckpointAsync(new LawnDart.Projections.ProjectionCheckpoint
            {
                ProjectionType = "OrderSummary:v1",
                NodeId = 0,
                LastSequencePosition = 8,
                LastUpdated = DateTime.UtcNow,
                TotalEventsProcessed = 8
            });
            Assert.Equal(8, (await waiterStore.GetCheckpointAsync("OrderSummary:v1", 0))!.LastSequencePosition);
            Assert.Equal(0, Volatile.Read(ref both));
            Assert.True(taken.IsHeld);
            Assert.False(leader.IsHeld);
        }
        finally
        {
            await watch.CancelAsync();
            try
            {
                await sampler;
            }
            catch (OperationCanceledException)
            {
                // The sampler stopped.
            }

            await leader.DisposeAsync();
            await taken.DisposeAsync();
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

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!condition())
            await Task.Delay(20, cts.Token);
    }
}

/// <summary>
/// PgBouncer in transaction mode. Runs when the Postgres path filter matches, not on every pull request.
/// </summary>
[Trait("Category", "Integration")]
[Trait("Category", "Pooler")]
public sealed class PostgresProjectionPoolerTests
{
    private const string BouncerImage =
        "edoburu/pgbouncer@sha256:8638aa563607256d3c7440437534195f1066a92b68015d89ae9d6b935b444a6d";

    [Fact(Timeout = 180000)]
    public async Task PoolerOnly_UsesALease_AndAllowsOneRunner()
    {
        await using var postgres = new PostgreSqlBuilder(PostgresTestImage.Current)
            .WithDatabase("lawndart")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .WithEnvironment("POSTGRES_HOST_AUTH_METHOD", "trust")
            .Build();
        await postgres.StartAsync();

        var postgresPort = postgres.GetMappedPublicPort(5432);
        await using var bouncer = new ContainerBuilder(BouncerImage)
            .WithExtraHost("host.docker.internal", "host-gateway")
            .WithEnvironment("DATABASE_URL", $"postgres://postgres:postgres@host.docker.internal:{postgresPort}/lawndart")
            .WithEnvironment("POOL_MODE", "transaction")
            .WithEnvironment("AUTH_TYPE", "trust")
            .WithEnvironment("LISTEN_PORT", "5432")
            .WithEnvironment("IGNORE_STARTUP_PARAMETERS", "extra_float_digits")
            .WithPortBinding(5432, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(5432))
            .Build();
        await bouncer.StartAsync();

        var pooled = new NpgsqlConnectionStringBuilder
        {
            Host = "127.0.0.1",
            Port = bouncer.GetMappedPublicPort(5432),
            Username = "postgres",
            Password = "postgres",
            Database = "lawndart",
            NoResetOnClose = true
        }.ConnectionString;
        var options = new PostgresProjectionStoreOptions
        {
            ConnectionString = pooled,
            MigrationConnectionString = postgres.GetConnectionString(),
            SchemaName = "pooled",
            LockProbeInterval = TimeSpan.FromMilliseconds(100),
            LeaseDuration = TimeSpan.FromSeconds(1),
            LeaseRenewInterval = TimeSpan.FromMilliseconds(200)
        };
        var views = new PostgresViewStore(options);
        var leaderStore = new PostgresCheckpointStore(options, views);
        var waiterStore = new PostgresCheckpointStore(options, views);
        await views.InitializeSchemaAsync();
        await leaderStore.InitializeSchemaAsync();
        var leaderLog = new ListLogger<PostgresProjectionRunnerLock>();
        var waiterLog = new ListLogger<PostgresProjectionRunnerLock>();
        var leaderLock = new PostgresProjectionRunnerLock(leaderStore, options, leaderLog);
        var waiterLock = new PostgresProjectionRunnerLock(waiterStore, options, waiterLog);
        var leader = (PostgresPooledLease)await leaderLock.AcquireAsync("OrderSummary:v1", 0);
        var pending = waiterLock.AcquireAsync("OrderSummary:v1", 0);
        try
        {
            Assert.Contains(leaderLog.Entries, entry => entry.Message.Contains("slower", StringComparison.Ordinal));
            await WaitUntilAsync(() => waiterLog.Entries.Any(entry =>
                entry.Message.Contains("share node number", StringComparison.Ordinal)));
            Assert.True(leader.IsHeld);
            Assert.False(pending.IsCompleted);
            leader.Abandon();
            var taken = await pending.WaitAsync(TimeSpan.FromSeconds(15));
            Assert.True(taken.IsHeld);
            Assert.False(leader.IsHeld);
            Assert.Null(taken.SessionProcessId);
            Assert.True(taken.Epoch > leader.Epoch);
            await taken.DisposeAsync();
        }
        finally
        {
            await leader.DisposeAsync();
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (!condition())
            await Task.Delay(20, cts.Token);
    }
}
