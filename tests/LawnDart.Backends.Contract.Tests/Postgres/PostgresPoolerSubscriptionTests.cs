using DotNet.Testcontainers.Builders;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventStore;
using Microsoft.Extensions.Logging;
using Npgsql;
using Testcontainers.PostgreSql;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// PgBouncer in transaction mode. Runs when the Postgres path filter matches, not on every pull request.
/// </summary>
[Trait("Category", "Integration")]
[Trait("Category", "Pooler")]
public sealed class PostgresPoolerSubscriptionTests
{
    private const string BouncerImage =
        "edoburu/pgbouncer@sha256:8638aa563607256d3c7440437534195f1066a92b68015d89ae9d6b935b444a6d";

    [Fact(Timeout = 180000)]
    public async Task TransactionPooler_LogsOneWarning_Polls_AndDoesNotRequireSessionState()
    {
        await using var postgres = new PostgreSqlBuilder(PostgresTestImage.Current)
            .WithDatabase("lawndart")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .WithEnvironment("POSTGRES_HOST_AUTH_METHOD", "trust")
            .Build();
        await postgres.StartAsync();

        var postgresPort = postgres.GetMappedPublicPort(5432);
        // The pooler reaches Postgres through the published port.
        // Transaction mode cannot keep a listen session.
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

        var connectionString = new NpgsqlConnectionStringBuilder
        {
            Host = "127.0.0.1",
            Port = bouncer.GetMappedPublicPort(5432),
            Username = "postgres",
            Password = "postgres",
            Database = "lawndart",
            NoResetOnClose = true
        }.ConnectionString;

        var logger = new ListLogger<PostgresEventStore>();
        var store = new PostgresEventStore(
            connectionString,
            options: new PostgresEventStoreOptions
            {
                ConnectionString = connectionString,
                SchemaName = "pooled",
                RequireTenantId = false,
                PollOnlyInterval = TimeSpan.FromMilliseconds(50),
                BackupPollInterval = TimeSpan.FromSeconds(30)
            },
            logger: logger);
        await store.InitializeSchemaAsync();

        var stream = "Order:" + Guid.NewGuid().ToString("N");
        await using var handle = store.Subscribe("pooled", fromSequence: 1);
        await WaitUntilAsync(() => store.Listener.Mode == PostgresListenMode.PollOnly);
        await store.AppendAsync(stream, [ContractEvent.Tick(), ContractEvent.Tick(), ContractEvent.Tick()]);
        var received = await ReadExactlyAsync(handle, 3, TimeSpan.FromSeconds(15));
        Assert.Equal(new long[] { 1, 2, 3 }, received.Select(e => e.SequencePosition));

        var warnings = logger.Entries.Count(entry =>
            entry.Level == LogLevel.Warning
            && entry.Message.Contains("Subscriptions are polling.", StringComparison.Ordinal));
        Assert.Equal(1, warnings);

        await using var again = new NpgsqlConnection(connectionString);
        await again.OpenAsync();
        await using var first = new NpgsqlCommand("SELECT 1", again);
        Assert.Equal(1, Convert.ToInt32(await first.ExecuteScalarAsync()));
        await using var second = new NpgsqlCommand("SELECT current_setting('server_version_num')", again);
        Assert.False(string.IsNullOrWhiteSpace((string)(await second.ExecuteScalarAsync())!));
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (!condition())
            await Task.Delay(20, cts.Token);
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
}
