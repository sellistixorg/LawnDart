using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart.EventStore;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Admin;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;
using LawnDart.Projections.Telemetry;

namespace LawnDart.Projections.Lightweight;

/// <summary>
/// Extension methods on <see cref="BoundedContextBuilder"/> provided by the
/// <c>LawnDart.Projections.Lightweight</c> package.
/// </summary>
public static class BoundedContextProjectionExtensions
{
    /// <summary>
    /// Scans the supplied assemblies for lightweight projection classes and registers a
    /// <see cref="LightweightProjectionRunnerService"/> for each, wired to the keyed
    /// <c>IEventStore</c>, <c>ICheckpointStore</c>, and <c>IViewStore</c> for this context.
    /// </summary>
    /// <param name="builder">The bounded context builder.</param>
    /// <param name="assemblies">
    /// One or more assemblies to scan.  Typically the assembly that contains your projection
    /// handler classes: <c>typeof(MyProjection).Assembly</c>.
    /// </param>
    /// <param name="configure">
    /// Optional callback to override <see cref="LightweightProjectionOptions"/> for this context.
    /// </param>
    /// <returns>The builder for further chaining.</returns>
    /// <remarks>
    /// Call <c>AddInMemoryProjectionStores(contextName)</c> or
    /// <c>AddSqlProjectionStores(contextName, connectionString)</c> before
    /// calling <c>WithProjections()</c> to ensure the keyed stores are registered.
    /// </remarks>
    public static BoundedContextBuilder WithProjections(
        this BoundedContextBuilder builder,
        Assembly[] assemblies,
        Action<LightweightProjectionOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var services    = builder.Services;
        var contextName = builder.ContextName;

        // Build options for this context (always has ContextName set)
        var options = new LightweightProjectionOptions { ContextName = contextName };
        configure?.Invoke(options);

        // Telemetry flags are process-wide (last registration wins in multi-context hosts).
        services.TryAddSingleton(options.Telemetry);
        ProjectionTelemetry.Configure(options.Telemetry);

        // HTTP context accessor (needed by JWT tenant provider)
        services.AddHttpContextAccessor();

        // Partitioning service (per-context instance keyed by contextName)
        services.AddKeyedSingleton<IPartitioningService>(contextName, (_, _) =>
            options.TotalInstances > 1
                ? (IPartitioningService)new ConsistentHashPartitioningService(options.NodeInstance, options.TotalInstances)
                : new SingleNodePartitioningService());

        // Hot read cache keyed per bounded context.
        services.AddKeyedSingleton<IProjectionReadCache>(contextName, (_, _) => new ProjectionReadCache(options));

        // Scan and register projection runners
        var registrations = ProjectionScanner.Scan(assemblies);

        // Accumulate registrations into the shared IReadOnlyList<ProjectionRegistration>
        // (AddLightweightProjections already handles this for the non-keyed path; here we
        // maintain a keyed list as well for per-context tooling).
        services.AddKeyedSingleton<IReadOnlyList<ProjectionRegistration>>(
            contextName, (_, _) => (IReadOnlyList<ProjectionRegistration>)registrations);
        services.AddKeyedSingleton<ProjectionRegistrationCatalog>(contextName, (sp, _) =>
        {
            var catalog = new ProjectionRegistrationCatalog(registrations);
            var logger = sp.GetService<Microsoft.Extensions.Logging.ILoggerFactory>()
                ?.CreateLogger("LawnDart.Projections.Lightweight");
            ProjectionRegistrationDiagnostics.LogLatestFallbackWarnings(logger, catalog, contextName);
            return catalog;
        });

        // ── Runner manager (keyed by context name) ────────────────────────────
        // Concrete type registered first so the hosted-service wrapper can resolve it directly.
        services.AddKeyedSingleton<BoundedContextProjectionRunnerManager>(contextName, (sp, _) =>
        {
            var opts = options; // capture
            return new BoundedContextProjectionRunnerManager(
                registrations,
                reg => new LightweightProjectionRunnerService(
                    reg,
                    sp.GetRequiredKeyedService<LawnDart.EventStore.IEventStore>(contextName),
                    sp.GetRequiredKeyedService<IViewStore>(contextName),
                    sp.GetRequiredKeyedService<ICheckpointStore>(contextName),
                    sp.GetRequiredKeyedService<IPartitioningService>(contextName),
                    opts,
                    sp.GetService<ILogger<LightweightProjectionRunnerService>>(),
                    sp.GetRequiredKeyedService<IProjectionReadCache>(contextName)),
                sp.GetService<ILogger<BoundedContextProjectionRunnerManager>>(),
                sp.GetRequiredKeyedService<LawnDart.EventStore.IEventStore>(contextName),
                opts);
        });

        // Interface key → delegates to the concrete registration above.
        services.AddKeyedSingleton<IProjectionRunnerManager>(contextName,
            (sp, _) => sp.GetRequiredKeyedService<BoundedContextProjectionRunnerManager>(contextName));

        // One thin hosted service per context boots all runners and shuts them down.
        services.AddSingleton<IHostedService>(sp =>
            new BoundedContextRunnerManagerHostedService(
                sp.GetRequiredKeyedService<BoundedContextProjectionRunnerManager>(contextName)));

        // ── Projection admin (keyed by context name) ──────────────────────────
        services.AddKeyedSingleton<IProjectionAdmin>(contextName, (sp, _) =>
            new BoundedContextProjectionAdmin(
                sp.GetRequiredKeyedService<IProjectionRunnerManager>(contextName),
                sp.GetRequiredKeyedService<ICheckpointStore>(contextName),
                sp.GetRequiredKeyedService<IViewStore>(contextName),
                sp.GetRequiredKeyedService<ProjectionRegistrationCatalog>(contextName),
                sp.GetRequiredKeyedService<IPartitioningService>(contextName),
                sp.GetService<ILogger<BoundedContextProjectionAdmin>>(),
                sp.GetRequiredKeyedService<IProjectionReadCache>(contextName)));

        // Shared marker for MapProjectionAdminApi / debugger Rebuild gating.
        services.TryAddSingleton<ProjectionAdminApiRegistration>();

        return builder;
    }

    /// <inheritdoc cref="WithProjections(BoundedContextBuilder, Assembly[], Action{LightweightProjectionOptions}?)"/>
    public static BoundedContextBuilder WithProjections(
        this BoundedContextBuilder builder,
        Action<LightweightProjectionOptions>? configure = null,
        params Assembly[] assemblies)
        => builder.WithProjections(assemblies, configure);

    // -------------------------------------------------------------------------
    // AddInMemoryProjectionStores (keyed variant)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Registers keyed in-memory <see cref="IViewStore"/> and <see cref="ICheckpointStore"/>
    /// for the specified bounded context.
    /// </summary>
    /// <remarks>
    /// Use this overload in multi-context applications or tests.  For single-context
    /// applications the unkeyed <c>AddInMemoryProjectionStores()</c> on
    /// <see cref="LightweightProjectionsExtensions"/> is sufficient.
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="contextName">The bounded context name (DI key).</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddInMemoryProjectionStores(
        this IServiceCollection services,
        string contextName)
    {
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", nameof(contextName));

        services.AddKeyedSingleton<IViewStore>(contextName, (_, _) => new InMemoryViewStore());
        services.AddKeyedSingleton<ICheckpointStore>(contextName, (sp, key) =>
            new InMemoryCheckpointStore(
                sp.GetRequiredKeyedService<IViewStore>(key!),
                sp.GetService<Microsoft.Extensions.Logging.ILogger<InMemoryCheckpointStore>>()));

        return services;
    }

    // -------------------------------------------------------------------------
    // AddSqlProjectionStores (keyed variant)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Registers keyed SQL Server <see cref="IViewStore"/> and <see cref="ICheckpointStore"/>
    /// for the specified bounded context, using schema-qualified tables.
    /// </summary>
    /// <remarks>
    /// Call this <b>before</b> <c>WithProjections(contextName)</c>.
    /// When <paramref name="schemaName"/> is null or whitespace, the SQL schema defaults to
    /// <paramref name="contextName"/> (multi-context convention). Pass <c>"dbo"</c> explicitly
    /// to keep the default schema while still using a non-dbo DI key.
    /// <para>
    /// After the host is built, call <see cref="InitializeSqlProjectionStoresAsync"/> once with
    /// an elevated connection so tables and indexes exist before runners start.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="contextName">The bounded context name (DI key).</param>
    /// <param name="connectionString">SQL Server connection string.</param>
    /// <param name="schemaName">
    /// Optional SQL schema. Defaults to <paramref name="contextName"/> when null/empty.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlProjectionStores(
        this IServiceCollection services,
        string contextName,
        string connectionString,
        string? schemaName = null)
    {
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", nameof(contextName));
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
    /// Prefer running this from an elevated deploy/admin job rather than the long-lived app
    /// process. After objects exist, re-runs are catalog no-ops (DDL branches skipped).
    /// </remarks>
    /// <param name="services">Built service provider.</param>
    /// <param name="contextName">Bounded context DI key.</param>
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
