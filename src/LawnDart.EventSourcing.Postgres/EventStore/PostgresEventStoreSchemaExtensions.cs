using LawnDart.EventStore;
using Microsoft.Extensions.DependencyInjection;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>Reads the Postgres event store registered for a bounded context.</summary>
public static class PostgresEventStoreSchemaExtensions
{
    /// <summary>
    /// Reads the schema of the <see cref="PostgresEventStore"/> registered for <paramref name="contextName"/>.
    /// </summary>
    /// <param name="services">Built service provider.</param>
    /// <param name="contextName">Bounded context dependency injection key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Whether that store's schema matches this version.</returns>
    /// <remarks>
    /// This method does not create or change objects. It uses the options registered for the context.
    /// Call it at startup. <see cref="SchemaCheckResult.ThrowIfMismatch"/> throws when the result does not match.
    /// </remarks>
    public static async Task<SchemaCheckResult> VerifyPostgresEventStoreAsync(
        this IServiceProvider services,
        string contextName,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", nameof(contextName));

        var store = services.GetRequiredKeyedService<IEventStore>(contextName);
        if (store is not PostgresEventStore postgres)
        {
            throw new InvalidOperationException(
                $"Keyed IEventStore '{contextName}' is {store.GetType().Name}, not PostgresEventStore. " +
                "Call UsePostgres before VerifyPostgresEventStoreAsync.");
        }

        return await postgres.VerifySchemaAsync(cancellationToken).ConfigureAwait(false);
    }
}
