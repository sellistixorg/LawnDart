using System.Net.Sockets;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventStore;
using Microsoft.Extensions.Logging;
using Npgsql;
using Testcontainers.PostgreSql;

namespace LawnDart.Backends.Contract.Tests;

[Trait("Category", "Integration")]
public sealed class PostgresAppendClosureTests
{
    [Fact]
    public async Task Restart_FirstAppendWithoutClearingThePool_RetriesOnceAndIsReadableOnce()
    {
        await using var postgres = await StartPostgresAsync();
        var logger = new ListLogger<PostgresEventStore>();
        var store = (await OpenStoreAsync(postgres.ConnectionString, logger)).Store;
        var warmed = "Order:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(warmed, [ContractEvent.Tick()]);
        Assert.Single(await store.ReadStreamAsync(warmed));

        await postgres.Container.StopAsync();
        await postgres.Container.StartAsync();
        await WaitUntilAcceptsConnectionsAsync(postgres.ConnectionString);

        var stream = "Order:" + Guid.NewGuid().ToString("N");
        var appended = await store.AppendAsync(stream, [ContractEvent.Tick()]);
        var read = await store.ReadStreamAsync(stream);
        Assert.Single(read);
        Assert.Equal(appended.SequencePositions[0], read[0].SequencePosition);
        Assert.Single(await store.ReadStreamAsync(warmed));
        var warnings = logger.Entries.Where(entry =>
            entry.Level == LogLevel.Warning
            && entry.Message.Contains("tried once more", StringComparison.Ordinal)).ToArray();
        Assert.Single(warnings);
    }

    [Fact]
    public async Task KillAtCommit_ThrowsUnknown_AndDoesNotRetry()
    {
        await using var postgres = await StartPostgresAsync(maxPoolSize: 10);
        var logger = new ListLogger<PostgresEventStore>();
        var store = (await OpenStoreAsync(postgres.ConnectionString, logger)).Store;
        var attempts = 0;
        store.CommitHook = async (transaction, cancellationToken) =>
        {
            Interlocked.Increment(ref attempts);
            var connection = transaction.Connection ?? throw new InvalidOperationException("The append connection is closed.");
            var pid = connection.ProcessID;
            await TerminateAndWaitUntilGoneAsync(postgres.ConnectionString, pid, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        };

        var stream = "Order:" + Guid.NewGuid().ToString("N");
        var ex = await Assert.ThrowsAsync<AppendOutcomeUnknownException>(() => store.AppendAsync(stream, [ContractEvent.Tick()]));
        Assert.Equal(1, attempts);
        Assert.NotNull(ex.InnerException);
        Assert.Empty(await store.ReadStreamAsync(stream));
        Assert.DoesNotContain(
            logger.Entries,
            entry => entry.Message.Contains("tried once more", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Restart_VersionConflict_StillThrowsConcurrencyException()
    {
        await using var postgres = await StartPostgresAsync();
        var store = (await OpenStoreAsync(postgres.ConnectionString, logger: null)).Store;
        var stream = "Order:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(stream, [ContractEvent.Tick()]);

        await postgres.Container.StopAsync();
        await postgres.Container.StartAsync();
        await WaitUntilAcceptsConnectionsAsync(postgres.ConnectionString);

        var ex = await Assert.ThrowsAsync<ConcurrencyException>(() =>
            store.AppendAsync(stream, [ContractEvent.Tick()], expectedVersion: 0));
        Assert.Contains("Expected version 0", ex.Message, StringComparison.Ordinal);
        Assert.Single(await store.ReadStreamAsync(stream));
    }

    [Fact]
    public async Task ClosureBeforeTheAppendReachesTheServer_RetriesAndSavesOnce()
    {
        await using var harness = await StartProxiedPostgresAsync(PostgresClosureProxy.Mode.BeforeAppendReachesServer);
        var logger = new ListLogger<PostgresEventStore>();
        var opened = await OpenStoreAsync(harness.ProxyConnectionString, logger);
        harness.Proxy.Arm();

        var stream = "Order:" + Guid.NewGuid().ToString("N");
        var appended = await opened.Store.AppendAsync(stream, [ContractEvent.Tick()]);
        var read = await opened.Store.ReadStreamAsync(stream);
        Assert.Single(read);
        Assert.Equal(appended.SequencePositions[0], read[0].SequencePosition);
        Assert.Equal(1, await CountRowsAsync(harness.DirectConnectionString, opened.SchemaName, stream));
        Assert.Contains(
            logger.Entries,
            entry => entry.Level == LogLevel.Warning && entry.Message.Contains("tried once more", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ClosureAfterCommit_ExpectedVersion_ReturnsTheSavedAppendOnce()
    {
        await using var harness = await StartProxiedPostgresAsync(PostgresClosureProxy.Mode.AfterCommit);
        var opened = await OpenStoreAsync(harness.ProxyConnectionString, logger: null);
        var stream = "Order:" + Guid.NewGuid().ToString("N");
        harness.Proxy.Arm(appendsToForwardFirst: 1);
        await opened.Store.AppendAsync(stream, [ContractEvent.Tick()]);

        var appended = await opened.Store.AppendAsync(stream, [ContractEvent.Tick()], expectedVersion: 1);
        var read = await opened.Store.ReadStreamAsync(stream);
        Assert.Equal(2, read.Count);
        Assert.Equal(appended.SequencePositions[0], read[1].SequencePosition);
        Assert.Equal(2, appended.LastStreamVersion);
        Assert.Equal(2, await CountRowsAsync(harness.DirectConnectionString, opened.SchemaName, stream));
    }

    [Fact]
    public async Task ClosureAfterCommit_WithoutExpectedVersion_ReturnsTheSavedAppendOnce()
    {
        await using var harness = await StartProxiedPostgresAsync(PostgresClosureProxy.Mode.AfterCommit);
        var opened = await OpenStoreAsync(harness.ProxyConnectionString, logger: null);
        harness.Proxy.Arm();

        var stream = "Order:" + Guid.NewGuid().ToString("N");
        var appended = await opened.Store.AppendAsync(stream, [ContractEvent.Tick()]);
        var read = await opened.Store.ReadStreamAsync(stream);
        Assert.Single(read);
        Assert.Equal(appended.SequencePositions[0], read[0].SequencePosition);
        Assert.Equal(1, await CountRowsAsync(harness.DirectConnectionString, opened.SchemaName, stream));
    }

    [Fact]
    public async Task ClosureAfterCommit_DcbConditionOnItsOwnTag_ReturnsTheSavedAppendOnce()
    {
        await using var harness = await StartProxiedPostgresAsync(PostgresClosureProxy.Mode.AfterCommit);
        var opened = await OpenStoreAsync(harness.ProxyConnectionString, logger: null);
        harness.Proxy.Arm();

        var tag = "own-" + Guid.NewGuid().ToString("N");
        var appended = await opened.Store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
            tags: [tag]);
        var read = await opened.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tag)));
        Assert.Single(read.Events);
        Assert.Equal(appended.SequencePositions[0], read.Events[0].SequencePosition);
        Assert.Equal(-1, appended.LastStreamVersion);
        Assert.Equal(1, await CountTaggedRowsAsync(harness.DirectConnectionString, opened.SchemaName, tag));
    }

    private static async Task<OpenedStore> OpenStoreAsync(
        string connectionString,
        ListLogger<PostgresEventStore>? logger)
    {
        var schema = "a" + Guid.NewGuid().ToString("N");
        var store = new PostgresEventStore(
            connectionString,
            options: new PostgresEventStoreOptions
            {
                ConnectionString = connectionString,
                SchemaName = schema,
                RequireTenantId = false
            },
            logger: logger);
        await store.InitializeSchemaAsync();
        return new OpenedStore(store, schema);
    }

    private static async Task<ProxiedPostgres> StartProxiedPostgresAsync(PostgresClosureProxy.Mode mode)
    {
        var hostPort = FreeTcpPort();
        var container = new PostgreSqlBuilder(PostgresTestImage.Current)
            .WithDatabase("lawndart")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .WithPortBinding(hostPort, 5432)
            .Build();
        await container.StartAsync();
        var direct = new NpgsqlConnectionStringBuilder
        {
            Host = "127.0.0.1",
            Port = hostPort,
            Username = "postgres",
            Password = "postgres",
            Database = "lawndart",
            SslMode = SslMode.Disable
        }.ConnectionString;
        var proxy = new PostgresClosureProxy("127.0.0.1", hostPort, mode);
        await proxy.StartAsync();
        var throughProxy = new NpgsqlConnectionStringBuilder(direct)
        {
            Port = proxy.Port,
            SslMode = SslMode.Disable
        }.ConnectionString;
        return new ProxiedPostgres(container, proxy, direct, throughProxy);
    }

    private static async Task<int> CountRowsAsync(string connectionString, string schema, string streamId)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"SELECT count(*) FROM {Quote(schema)}.events WHERE stream_id = @id",
            connection);
        command.Parameters.AddWithValue("id", streamId);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    private static async Task<int> CountTaggedRowsAsync(string connectionString, string schema, string tag)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"SELECT count(*) FROM {Quote(schema)}.events WHERE @tag = ANY(tags)",
            connection);
        command.Parameters.AddWithValue("tag", tag);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static async Task<StartedPostgres> StartPostgresAsync(int maxPoolSize = 2)
    {
        var hostPort = FreeTcpPort();
        var container = new PostgreSqlBuilder(PostgresTestImage.Current)
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
            Database = "lawndart",
            Pooling = true,
            MaxPoolSize = maxPoolSize,
            Timeout = 15,
            CommandTimeout = 15,
            KeepAlive = 0
        }.ConnectionString;
        return new StartedPostgres(container, connectionString);
    }

    private static async Task WaitUntilAcceptsConnectionsAsync(string connectionString)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (true)
        {
            timeout.Token.ThrowIfCancellationRequested();
            try
            {
                await using var connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(timeout.Token);
                await using var command = new NpgsqlCommand("SELECT 1", connection);
                var one = (int)(await command.ExecuteScalarAsync(timeout.Token))!;
                if (one == 1)
                    return;
            }
            catch (Exception ex) when (ex is NpgsqlException or SocketException or IOException or TimeoutException)
            {
            }

            await Task.Delay(20, timeout.Token);
        }
    }

    private static async Task TerminateAndWaitUntilGoneAsync(
        string connectionString,
        int pid,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using (var kill = new NpgsqlCommand("SELECT pg_terminate_backend(@pid)", connection))
        {
            kill.Parameters.AddWithValue("pid", pid);
            var terminated = await kill.ExecuteScalarAsync(cancellationToken);
            if (terminated is not true)
                throw new InvalidOperationException("pg_terminate_backend did not accept the append backend.");
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(15));
        while (true)
        {
            timeout.Token.ThrowIfCancellationRequested();
            await using var check = new NpgsqlCommand(
                "SELECT pid FROM pg_stat_activity WHERE pid = @pid",
                connection);
            check.Parameters.AddWithValue("pid", pid);
            var stillThere = await check.ExecuteScalarAsync(timeout.Token);
            if (stillThere is null or DBNull)
                return;
            await Task.Delay(10, timeout.Token);
        }
    }

    private static int FreeTcpPort()
    {
        var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class StartedPostgres : IAsyncDisposable
    {
        public StartedPostgres(PostgreSqlContainer container, string connectionString)
        {
            Container = container;
            ConnectionString = connectionString;
        }

        public PostgreSqlContainer Container { get; }

        public string ConnectionString { get; }

        public ValueTask DisposeAsync() => Container.DisposeAsync();
    }

    private sealed class ProxiedPostgres : IAsyncDisposable
    {
        public ProxiedPostgres(
            PostgreSqlContainer container,
            PostgresClosureProxy proxy,
            string directConnectionString,
            string proxyConnectionString)
        {
            Container = container;
            Proxy = proxy;
            DirectConnectionString = directConnectionString;
            ProxyConnectionString = proxyConnectionString;
        }

        public PostgreSqlContainer Container { get; }

        public PostgresClosureProxy Proxy { get; }

        public string DirectConnectionString { get; }

        public string ProxyConnectionString { get; }

        public async ValueTask DisposeAsync()
        {
            await Proxy.DisposeAsync();
            await Container.DisposeAsync();
        }
    }

    private sealed class OpenedStore
    {
        public OpenedStore(PostgresEventStore store, string schemaName)
        {
            Store = store;
            SchemaName = schemaName;
        }

        public PostgresEventStore Store { get; }

        public string SchemaName { get; }
    }
}
