using LawnDart.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace LawnDart.Messaging.Postgres;

/// <summary>
/// DI registration for <see cref="PostgresInboxStore"/>.
/// </summary>
public static class PostgresInboxStoreExtensions
{
    /// <summary>
    /// Registers <see cref="PostgresInboxStore"/> as <see cref="IInboxStore"/>.
    /// Replaces an inbox already registered by a transport package, before or after that call.
    /// The registered <see cref="IMessageTransport"/> stays in place, including the
    /// in-process, RabbitMQ, Azure Service Bus, and Amazon SQS transports.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">Postgres connection string.</param>
    /// <param name="configure">Optional schema and table overrides. The connection string is set first.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddPostgresInboxStore(
        this IServiceCollection services,
        string connectionString,
        Action<PostgresInboxStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string must not be null or whitespace.", nameof(connectionString));

        services.AddMessaging();

        services.AddOptions<PostgresInboxStoreOptions>()
            .Configure(options =>
            {
                options.ConnectionString = connectionString;
                configure?.Invoke(options);
            });

        services.RemoveAll<PostgresInboxStore>();
        services.RemoveAll<IInboxStore>();
        services.AddSingleton<PostgresInboxStore>();
        services.AddSingleton<IInboxStore>(sp => sp.GetRequiredService<PostgresInboxStore>());
        return services;
    }

    /// <summary>
    /// Creates the inbox table for the <see cref="PostgresInboxStore"/> registered by
    /// <see cref="AddPostgresInboxStore"/>.
    /// </summary>
    /// <param name="services">Built service provider.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task InitializePostgresInboxStoreAsync(
        this IServiceProvider services,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(services);

        var store = services.GetRequiredService<IInboxStore>();
        if (store is not PostgresInboxStore postgres)
        {
            throw new InvalidOperationException(
                $"IInboxStore is {store.GetType().Name}, not PostgresInboxStore. " +
                "Call AddPostgresInboxStore(connectionString) before InitializePostgresInboxStoreAsync.");
        }

        await postgres.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
    }
}
