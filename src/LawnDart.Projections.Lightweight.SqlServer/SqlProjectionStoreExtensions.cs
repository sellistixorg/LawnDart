using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.SqlServer;

/// <summary>
/// Registers SQL Server projection view and checkpoint stores.
/// </summary>
public static class SqlProjectionStoreExtensions
{
    /// <summary>
    /// Registers keyed SQL Server <see cref="IViewStore"/> and <see cref="ICheckpointStore"/>
    /// for the specified bounded context, using schema-qualified tables.
    /// </summary>
    /// <remarks>
    /// Call this <b>before</b> <c>WithProjections</c>.
    /// When <paramref name="schemaName"/> is null or whitespace, the SQL schema defaults to
    /// <paramref name="contextName"/>. Pass <c>"dbo"</c> explicitly
    /// to keep that schema while still using a non-dbo dependency injection key.
    /// <paramref name="contextName"/> is checked with <see cref="LawnDartNames"/>.
    /// <para>
    /// Call <see cref="InitializeSqlProjectionStoresAsync"/> from a deploy or admin step,
    /// once per deploy, with a connection that can change the schema, so tables and indexes
    /// exist before runners start.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="contextName">The bounded context name (dependency injection key).</param>
    /// <param name="connectionString">SQL Server connection string.</param>
    /// <param name="schemaName">
    /// Optional SQL schema. Defaults to <paramref name="contextName"/> when null or empty.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlProjectionStores(
        this IServiceCollection services,
        string contextName,
        string connectionString,
        string? schemaName = null)
    {
        LawnDartNames.RequireContextName(services, contextName, nameof(contextName));
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string must not be null or whitespace.", nameof(connectionString));

        var effectiveSchema = string.IsNullOrWhiteSpace(schemaName) ? contextName : schemaName;

        services.AddKeyedSingleton<IViewStore>(contextName, (sp, _) =>
            new SqlViewStore(
                connectionString,
                sp.GetService<ILogger<SqlViewStore>>(),
                effectiveSchema));

        services.AddKeyedSingleton<ICheckpointStore>(contextName, (sp, key) =>
            new SqlCheckpointStore(
                connectionString,
                sp.GetRequiredKeyedService<IViewStore>(key!),
                sp.GetService<ILogger<SqlCheckpointStore>>(),
                effectiveSchema));

        return services;
    }

    /// <summary>
    /// Initializes schema for the keyed SQL <see cref="IViewStore"/> and
    /// <see cref="ICheckpointStore"/> registered by <see cref="AddSqlProjectionStores"/>.
    /// </summary>
    /// <remarks>
    /// Run this from a deploy step. Normal app startup should not call it.
    /// </remarks>
    /// <param name="services">Built service provider.</param>
    /// <param name="contextName">Bounded context dependency injection key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task InitializeSqlProjectionStoresAsync(
        this IServiceProvider services,
        string contextName,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", nameof(contextName));

        var viewStore = services.GetRequiredKeyedService<IViewStore>(contextName);
        var checkpointStore = services.GetRequiredKeyedService<ICheckpointStore>(contextName);

        if (viewStore is not SqlViewStore sqlViewStore)
        {
            throw new InvalidOperationException(
                $"Keyed IViewStore '{contextName}' is {viewStore.GetType().Name}, not SqlViewStore. " +
                "Call AddSqlProjectionStores(contextName, connectionString) before InitializeSqlProjectionStoresAsync.");
        }

        if (checkpointStore is not SqlCheckpointStore sqlCheckpointStore)
        {
            throw new InvalidOperationException(
                $"Keyed ICheckpointStore '{contextName}' is {checkpointStore.GetType().Name}, not SqlCheckpointStore. " +
                "Call AddSqlProjectionStores(contextName, connectionString) before InitializeSqlProjectionStoresAsync.");
        }

        await sqlViewStore.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        await sqlCheckpointStore.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
    }
}
