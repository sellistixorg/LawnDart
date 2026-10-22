using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.Postgres.Outbox;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.Outbox;
using LawnDart.Projections.Lightweight.Postgres;
using LawnDart.Messaging.Postgres;
using Microsoft.Extensions.DependencyInjection;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// The startup check from docs/SCHEMA_INITIALIZATION.md. This type exists so that sample compiles.
/// </summary>
public static class SchemaCheckStartupSample
{
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
