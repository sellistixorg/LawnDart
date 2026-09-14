using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Lightweight;
using LawnDart.Snapshots;
using LawnDart.TestUtilities;

namespace LawnDart.Backends.Contract.Tests.SwapApp;

/// <summary>
/// Same application registrations on both backends. Only the store and projection-store
/// calls differ — that is the swap the contract test proves.
/// </summary>
internal sealed class SwapHost : IAsyncDisposable
{
    public const string ContextName = "default";
    public const string RosterStorageKey = "StudentRoster:v1";

    private readonly ServiceProvider _services;
    private readonly IHostedService[] _hosted;

    private SwapHost(ServiceProvider services)
    {
        _services = services;
        _hosted = services.GetServices<IHostedService>().ToArray();
    }

    public IServiceProvider Services => _services;

    public static async Task<SwapHost> StartInMemoryAsync()
    {
        var services = CreateBaseServices();
        services.AddInMemoryProjectionStores(ContextName);

        var ctx = AddApplication(services);
        ctx.UseInMemory()
            .WithProjections(
                [typeof(StudentRosterProjection).Assembly],
                o =>
                {
                    o.PollInterval = TimeSpan.FromMilliseconds(50);
                    o.CheckpointInterval = 1;
                });

        services.AddSingleton<ISnapshotStrategyResolver>(_ =>
        {
            var resolver = new SnapshotStrategyResolver();
            resolver.RegisterForAggregate<Student>(new EventCountSnapshotStrategy(1));
            return resolver;
        });

        var host = new SwapHost(services.BuildServiceProvider());
        await host.StartAsync().ConfigureAwait(false);
        return host;
    }

    public static async Task<SwapHost> StartSqlServerAsync(string connectionString)
    {
        var services = CreateBaseServices();
        services.AddSqlProjectionStores(ContextName, connectionString, schemaName: "contract");

        var ctx = AddApplication(services);
        ctx.UseSqlServer(o =>
            {
                o.ConnectionString = connectionString;
                o.RequireTenantId = false;
                o.SchemaName = "contract";
            })
            .WithSnapshots(r => r.RegisterForAggregate<Student>(new EventCountSnapshotStrategy(1)))
            .WithProjections(
                [typeof(StudentRosterProjection).Assembly],
                o =>
                {
                    o.PollInterval = TimeSpan.FromMilliseconds(50);
                    o.CheckpointInterval = 1;
                });

        var built = services.BuildServiceProvider();
        var store = built.GetRequiredKeyedService<IEventStore>(ContextName);
        if (store is not SqlServerEventStore sql)
            throw new InvalidOperationException("SQL swap host did not resolve SqlServerEventStore.");

        await sql.InitializeSchemaAsync().ConfigureAwait(false);
        await built.InitializeSqlProjectionStoresAsync(ContextName).ConfigureAwait(false);

        var host = new SwapHost(built);
        await host.StartAsync().ConfigureAwait(false);
        return host;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var hosted in _hosted.Reverse())
            await hosted.StopAsync(CancellationToken.None).ConfigureAwait(false);
        await _services.DisposeAsync().ConfigureAwait(false);
    }

    private async Task StartAsync()
    {
        foreach (var hosted in _hosted)
            await hosted.StartAsync(CancellationToken.None).ConfigureAwait(false);
    }

    private static ServiceCollection CreateBaseServices()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddLawnDart(o =>
        {
            o.RequireTenantId = false;
            o.EnableAuthorization = false;
        });
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        return services;
    }

    private static BoundedContextBuilder AddApplication(IServiceCollection services)
        => services.AddBoundedContext(ContextName)
            .WithEventTypes<StudentEnrolled>()
            .WithCommandHandlers<EnrollStudentHandler>();
}
