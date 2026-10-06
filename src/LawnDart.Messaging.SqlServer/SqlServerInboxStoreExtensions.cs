using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace LawnDart.Messaging.SqlServer;

/// <summary>
/// DI registration for <see cref="SqlServerInboxStore"/>.
/// </summary>
public static class SqlServerInboxStoreExtensions
{
    /// <summary>
    /// Registers <see cref="SqlServerInboxStore"/> as <see cref="IInboxStore"/>.
    /// Replaces an inbox registered by <c>AddInMemoryMessaging</c>, before or after that call.
    /// The in-memory transport is left in place.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">SQL Server connection string.</param>
    /// <param name="configure">Optional schema and table overrides. The connection string is set first.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlInboxStore(
        this IServiceCollection services,
        string connectionString,
        Action<SqlServerInboxStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string must not be null or whitespace.", nameof(connectionString));

        services.AddMessaging();

        services.AddOptions<SqlServerInboxStoreOptions>()
            .Configure(options =>
            {
                options.ConnectionString = connectionString;
                configure?.Invoke(options);
            });

        services.RemoveAll<SqlServerInboxStore>();
        services.RemoveAll<IInboxStore>();
        services.AddSingleton<SqlServerInboxStore>();
        services.AddSingleton<IInboxStore>(sp => sp.GetRequiredService<SqlServerInboxStore>());
        return services;
    }

    /// <summary>
    /// Creates the inbox table for the <see cref="SqlServerInboxStore"/> registered by
    /// <see cref="AddSqlInboxStore"/>.
    /// </summary>
    /// <param name="services">Built service provider.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task InitializeSqlInboxStoreAsync(
        this IServiceProvider services,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(services);

        var store = services.GetRequiredService<IInboxStore>();
        if (store is not SqlServerInboxStore sql)
        {
            throw new InvalidOperationException(
                $"IInboxStore is {store.GetType().Name}, not SqlServerInboxStore. " +
                "Call AddSqlInboxStore(connectionString) before InitializeSqlInboxStoreAsync.");
        }

        await sql.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
    }
}
