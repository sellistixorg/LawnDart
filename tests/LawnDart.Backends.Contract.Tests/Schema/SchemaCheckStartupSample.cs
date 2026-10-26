using LawnDart;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.Postgres.Outbox;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Messaging.Postgres;
using LawnDart.Outbox;
using LawnDart.Projections.Lightweight.Postgres;
using LawnDart.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// The startup check from docs/SCHEMA_INITIALIZATION.md. This type exists so that sample compiles.
/// </summary>
public static class SchemaCheckStartupSample
{
    public static void Register(IServiceCollection services, string appConnectionString)
    {
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddPostgresProjectionStores("default", appConnectionString);
        services.AddPostgresInboxStore(appConnectionString);
        services.AddBoundedContext("default")
            .UsePostgres(o =>
            {
                o.ConnectionString = appConnectionString;
                o.RequireTenantId = false;
                o.EnableOutbox = true;
            })
            .WithEventTypes<CounterCreated>();
    }

    public static async Task CheckPostgresAsync(IServiceProvider provider, CancellationToken cancellationToken = default)
    {
        (await provider.VerifyPostgresEventStoreAsync("default", cancellationToken)).ThrowIfMismatch();

        var outbox = (PostgresOutboxWriter)provider.GetRequiredKeyedService<IOutboxWriter>("default");
        (await outbox.VerifySchemaAsync(cancellationToken)).ThrowIfMismatch();

        (await provider.VerifyPostgresProjectionStoresAsync("default", cancellationToken)).ThrowIfMismatch();
        (await provider.VerifyPostgresInboxStoreAsync(cancellationToken)).ThrowIfMismatch();
    }

    public static async Task CheckSqlServerAsync(IServiceProvider provider, CancellationToken cancellationToken = default)
    {
        (await provider.VerifySqlServerEventStoreAsync("default", cancellationToken)).ThrowIfMismatch();
    }
}

public class SchemaCheckStartupSampleTests
{
    [Fact]
    public void Register_ResolvesThePostgresEventStore()
    {
        var services = new ServiceCollection();
        SchemaCheckStartupSample.Register(
            services,
            "Host=127.0.0.1;Username=lawndart;Password=lawndart;Database=lawndart");
        using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredKeyedService<IEventStore>("default");

        Assert.IsType<PostgresEventStore>(store);
    }
}
