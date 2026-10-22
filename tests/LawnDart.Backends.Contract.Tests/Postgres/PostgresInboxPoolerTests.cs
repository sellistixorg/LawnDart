using DotNet.Testcontainers.Builders;
using LawnDart.Messaging;
using LawnDart.Messaging.Postgres;
using Microsoft.Extensions.Options;
using Npgsql;
using Testcontainers.PostgreSql;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Inbox claim through PgBouncer in transaction mode.
/// Runs when the Postgres path filter matches, not on every pull request.
/// </summary>
[Trait("Category", "Integration")]
[Trait("Category", "Pooler")]
public sealed class PostgresInboxPoolerTests
{
    private const string BouncerImage =
        "edoburu/pgbouncer@sha256:8638aa563607256d3c7440437534195f1066a92b68015d89ae9d6b935b444a6d";

    [Fact(Timeout = 180000)]
    public async Task TryClaim_WorksThroughTransactionMode()
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

        var direct = postgres.GetConnectionString();
        var pooled = new NpgsqlConnectionStringBuilder
        {
            Host = "127.0.0.1",
            Port = bouncer.GetMappedPublicPort(5432),
            Username = "postgres",
            Password = "postgres",
            Database = "lawndart",
            NoResetOnClose = true
        }.ConnectionString;

        var schema = "c" + Guid.NewGuid().ToString("N");
        var messaging = Options.Create(new MessagingOptions { InboxClaimLease = TimeSpan.FromHours(1) });
        var setup = new PostgresInboxStore(
            Options.Create(new PostgresInboxStoreOptions
            {
                ConnectionString = direct,
                SchemaName = schema
            }),
            messaging);
        await setup.InitializeSchemaAsync();

        var inbox = new PostgresInboxStore(
            Options.Create(new PostgresInboxStoreOptions
            {
                ConnectionString = pooled,
                SchemaName = schema
            }),
            messaging);

        Assert.True(await inbox.TryClaimAsync("pool-1"));
        Assert.False(await inbox.TryClaimAsync("pool-1"));

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = Enumerable.Range(0, 2).Select(async _ =>
        {
            await gate.Task;
            return await inbox.TryClaimAsync("pool-race");
        }).ToArray();
        gate.SetResult();
        var claimed = await Task.WhenAll(tasks);
        Assert.Equal(1, claimed.Count(won => won));

        await inbox.ReleaseClaimAsync("pool-1");
        Assert.True(await inbox.TryClaimAsync("pool-1"));
        await inbox.MarkProcessedAsync("pool-1", DateTimeOffset.UtcNow);
        Assert.False(await inbox.TryClaimAsync("pool-1"));
        Assert.True(await inbox.IsProcessedAsync("pool-1"));
    }
}
