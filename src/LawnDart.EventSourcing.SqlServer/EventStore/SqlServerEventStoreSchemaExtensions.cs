using LawnDart.EventStore;
using Microsoft.Extensions.DependencyInjection;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>Reads the SQL Server event store registered for a bounded context.</summary>
public static class SqlServerEventStoreSchemaExtensions
{
    /// <summary>
    /// Reads the schema of the <see cref="SqlServerEventStore"/> registered for <paramref name="contextName"/>.
    /// </summary>
    /// <param name="services">Built service provider.</param>
    /// <param name="contextName">Bounded context dependency injection key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Whether that store's schema matches this version.</returns>
    /// <remarks>
    /// This method does not create or change objects. It uses the options registered for the context.
    /// Call it at startup. <see cref="SchemaCheckResult.ThrowIfMismatch"/> throws when the result does not match.
    /// </remarks>
    public static async Task<SchemaCheckResult> VerifySqlServerEventStoreAsync(
        this IServiceProvider services,
        string contextName,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", nameof(contextName));

        var store = services.GetRequiredKeyedService<IEventStore>(contextName);
        if (store is not SqlServerEventStore sql)
        {
            throw new InvalidOperationException(
                $"Keyed IEventStore '{contextName}' is {store.GetType().Name}, not SqlServerEventStore. " +
                "Call UseSqlServer before VerifySqlServerEventStoreAsync.");
        }

        return await sql.VerifySchemaAsync(cancellationToken).ConfigureAwait(false);
    }
}
