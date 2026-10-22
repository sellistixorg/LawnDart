using LawnDart;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Postgres;

/// <summary>
/// Registers Postgres projection view and checkpoint stores.
/// </summary>
public static class PostgresProjectionStoreExtensions
{
    /// <summary>
    /// Registers keyed Postgres <see cref="IViewStore"/>, <see cref="ICheckpointStore"/>,
    /// and, unless opted out, <see cref="IProjectionRunnerLock"/>.
    /// </summary>
    /// <remarks>
    /// Call this before <c>WithProjections</c>. When <paramref name="schemaName"/> is null
    /// or whitespace, the <c>default</c> context uses schema <c>lawndart</c> and any other
    /// context uses its own name. The <c>public</c> schema is rejected.
    /// <paramref name="configure"/> can set <see cref="PostgresProjectionStoreOptions.ListenConnectionString"/>,
    /// <see cref="PostgresProjectionStoreOptions.MigrationConnectionString"/>, and
    /// <see cref="PostgresProjectionStoreOptions.EnableSingleRunnerLock"/>.
    /// The single-runner lock is on unless that flag is set false.
    /// Call <see cref="InitializePostgresProjectionStoresAsync"/> from a deploy or admin
    /// step, once per deploy, so the tables exist before runners start.
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="contextName">The bounded context name (dependency injection key).</param>
    /// <param name="connectionString">Postgres connection string for reads and writes.</param>
    /// <param name="schemaName">Optional schema. See the remarks for the default.</param>
    /// <param name="configure">Optional override for lock and connection settings.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddPostgresProjectionStores(
        this IServiceCollection services,
        string contextName,
        string connectionString,
        string? schemaName = null,
        Action<PostgresProjectionStoreOptions>? configure = null)
    {
        LawnDartNames.RequireContextName(services, contextName, nameof(contextName));
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string must not be null or whitespace.", nameof(connectionString));

        var options = new PostgresProjectionStoreOptions
        {
            ConnectionString = connectionString,
            SchemaName = string.IsNullOrWhiteSpace(schemaName)
                ? SchemaFor(contextName)
                : schemaName
        };
        configure?.Invoke(options);
        options.EnsureValid();

        services.AddKeyedSingleton(contextName, options);
        services.AddKeyedSingleton<PostgresViewStore>(contextName, (sp, _) =>
            new PostgresViewStore(options, sp.GetService<ILogger<PostgresViewStore>>()));
        services.AddKeyedSingleton<PostgresCheckpointStore>(contextName, (sp, key) =>
            new PostgresCheckpointStore(
                options,
                sp.GetRequiredKeyedService<PostgresViewStore>(key!),
                sp.GetService<ILogger<PostgresCheckpointStore>>()));
        services.AddKeyedSingleton<PostgresProjectionStore>(contextName, (sp, key) =>
            new PostgresProjectionStore(
                sp.GetRequiredKeyedService<PostgresViewStore>(key!),
                sp.GetRequiredKeyedService<PostgresCheckpointStore>(key!)));
        services.AddKeyedSingleton<IViewStore>(contextName, (sp, key) =>
            sp.GetRequiredKeyedService<PostgresProjectionStore>(key!));
        services.AddKeyedSingleton<ICheckpointStore>(contextName, (sp, key) =>
            sp.GetRequiredKeyedService<PostgresProjectionStore>(key!));
        services.AddKeyedSingleton<IAtomicProjectionFlush>(contextName, (sp, key) =>
            sp.GetRequiredKeyedService<PostgresProjectionStore>(key!));

        if (options.EnableSingleRunnerLock)
        {
            services.AddKeyedSingleton<IProjectionRunnerLock>(contextName, (sp, key) =>
                new PostgresProjectionRunnerLock(
                    sp.GetRequiredKeyedService<PostgresCheckpointStore>(key!),
                    options,
                    sp.GetService<ILogger<PostgresProjectionRunnerLock>>()));
        }

        return services;
    }

    /// <summary>
    /// Creates the keyed Postgres view and checkpoint tables registered by
    /// <see cref="AddPostgresProjectionStores"/>.
    /// </summary>
    /// <remarks>
    /// Run this from a deploy job, a migration step, or an admin tool, once per deploy.
    /// Normal app startup should not call it. An older copy that restarts and runs it
    /// can change a schema a newer version already upgraded.
    /// Prefer a connection that can create tables. After the tables exist, a later call
    /// does not create them again. The flush function is replaced with the current body.
    /// </remarks>
    /// <param name="services">Built service provider.</param>
    /// <param name="contextName">Bounded context dependency injection key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task InitializePostgresProjectionStoresAsync(
        this IServiceProvider services,
        string contextName,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", nameof(contextName));

        var views = services.GetRequiredKeyedService<IViewStore>(contextName);
        if (views is not PostgresProjectionStore store)
        {
            throw new InvalidOperationException(
                $"Keyed IViewStore '{contextName}' is {views.GetType().Name}, not PostgresProjectionStore. " +
                "Call AddPostgresProjectionStores before InitializePostgresProjectionStoresAsync.");
        }

        await store.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the keyed Postgres projection schema registered by <see cref="AddPostgresProjectionStores"/>.
    /// </summary>
    /// <param name="services">Built service provider.</param>
    /// <param name="contextName">Bounded context dependency injection key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Whether the projection schema matches this version.</returns>
    /// <remarks>
    /// This method does not create or change objects. Call it at startup.
    /// <see cref="SchemaCheckResult.ThrowIfMismatch"/> throws when the result does not match.
    /// </remarks>
    public static async Task<SchemaCheckResult> VerifyPostgresProjectionStoresAsync(
        this IServiceProvider services,
        string contextName,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", nameof(contextName));

        var views = services.GetRequiredKeyedService<IViewStore>(contextName);
        if (views is not PostgresProjectionStore store)
        {
            throw new InvalidOperationException(
                $"Keyed IViewStore '{contextName}' is {views.GetType().Name}, not PostgresProjectionStore. " +
                "Call AddPostgresProjectionStores before VerifyPostgresProjectionStoresAsync.");
        }

        return await store.VerifySchemaAsync(cancellationToken).ConfigureAwait(false);
    }

    private static string SchemaFor(string contextName)
        => string.Equals(contextName, LawnDartNames.DefaultContextName, StringComparison.Ordinal)
            ? "lawndart"
            : contextName;
}
