using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.Demo.ECommerce.Dcb;
using LawnDart.Demo.ECommerce.Domain.Cart.Events;
using LawnDart.Demo.ECommerce.EDA;
using LawnDart.Demo.ECommerce.MultiContext;
using LawnDart.Demo.ECommerce.Projections;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Outbox;
using Testcontainers.MsSql;

namespace LawnDart.Demo.ECommerce;

/// <summary>
/// Cart, product, and order console.
///
/// Usage:
///   dotnet run --project demos/LawnDart.Demo.ECommerce -- traditional
///   dotnet run --project demos/LawnDart.Demo.ECommerce -- dcb
///   dotnet run --project demos/LawnDart.Demo.ECommerce -- hybrid
///   dotnet run --project demos/LawnDart.Demo.ECommerce -- eda
///   dotnet run --project demos/LawnDart.Demo.ECommerce -- multicontext
///   dotnet run --project demos/LawnDart.Demo.ECommerce -- outbox --backend sqlserver
///   dotnet run --project demos/LawnDart.Demo.ECommerce -- --run-all
///
/// Backend is InMemory unless --backend sqlserver is set.
/// The eda approach stays in-process and does not open that store.
/// SQL uses LAWNDART_SQL_CONNECTION or ConnectionStrings:ECommerce.
/// When neither is set, --backend sqlserver starts a local container.
/// </summary>
internal static class Program
{
    // DEMO ONLY. The container accepts this password for the local SQL run.
    private const string DemoSqlPassword = "Your_password123";
    private const string DemoSqlImage = "mcr.microsoft.com/mssql/server:2022-CU20-ubuntu-22.04";

    private static readonly string[] Approaches =
        ["traditional", "dcb", "hybrid", "eda", "multicontext", "outbox"];

    public static async Task<int> Main(string[] args)
    {
        var runAll = args.Any(a => a.Equals("--run-all", StringComparison.OrdinalIgnoreCase));
        var backend = "inmemory";
        var positional = new List<string>();
        for (var i = 0; i < args.Length; i++)
        {
            if (args[i].Equals("--backend", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                backend = args[++i].ToLowerInvariant();
                continue;
            }

            if (args[i].Equals("--sql", StringComparison.OrdinalIgnoreCase))
            {
                backend = "sqlserver";
                continue;
            }

            if (args[i].StartsWith('-'))
                continue;

            positional.Add(args[i].ToLowerInvariant());
        }

        var approach = positional.Count > 0 ? positional[0] : "traditional";
        if (!runAll && !Approaches.Contains(approach))
        {
            PrintUsage();
            return 1;
        }

        if (backend is not ("inmemory" or "sqlserver"))
        {
            Console.WriteLine($"Unknown backend '{backend}'. Use inmemory or sqlserver.");
            return 1;
        }

        if (approach == "outbox" && backend != "sqlserver")
        {
            Console.WriteLine("The outbox run writes SQL Server rows. Pass --backend sqlserver.");
            Console.WriteLine("InMemory does not commit an outbox row with the event.");
            return 1;
        }

        var edaOnly = !runAll && approach == "eda";
        Console.WriteLine("=== LawnDart ECommerce ===");
        Console.WriteLine(runAll ? "Run: store scenarios, plus outbox when the backend is SQL Server." : $"Approach: {approach}");
        if (edaOnly)
            Console.WriteLine("Backend: in-process. eda does not use the event store.");
        else
            Console.WriteLine($"Backend: {backend}");
        Console.WriteLine();

        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        string? connectionString = null;
        MsSqlContainer? container = null;
        if (backend == "sqlserver" && !edaOnly)
        {
            connectionString = config.GetConnectionString("ECommerce")
                ?? Environment.GetEnvironmentVariable("LAWNDART_SQL_CONNECTION");
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                Console.WriteLine("No connection string. Starting a local SQL Server container.");
                Console.WriteLine("Set LAWNDART_SQL_CONNECTION to skip the container.");
                container = new MsSqlBuilder(DemoSqlImage).WithPassword(DemoSqlPassword).Build();
                await container.StartAsync();
                connectionString = container.GetConnectionString();
            }
        }

        try
        {
            var steps = runAll
                ? backend == "sqlserver"
                    ? Approaches
                    : Approaches.Where(a => a != "outbox").ToArray()
                : [approach];

            foreach (var step in steps)
            {
                if (runAll)
                    Console.WriteLine($"--- {step} ---");

                if (step == "multicontext")
                {
                    await MultiContextDemo.RunAsync(connectionString);
                    continue;
                }

                if (step == "eda")
                {
                    if (backend == "sqlserver")
                        Console.WriteLine("eda stays in-process. It does not use the SQL store.");
                    await new EDADemo().RunAsync();
                    continue;
                }

                await RunStoreScenarioAsync(step, connectionString);
            }

            Console.WriteLine();
            Console.WriteLine("=== Demo complete ===");
            return 0;
        }
        finally
        {
            if (container is not null)
                await container.DisposeAsync();
        }
    }

    private static async Task RunStoreScenarioAsync(string approach, string? connectionString)
    {
        var useSql = connectionString is not null;
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        services.AddLawnDart(options =>
        {
            options.RequireTenantId = false;
            options.EnableAuthorization = false;
        });
        services.AddSingleton<ITenantContextProvider>(new DemoTenantContextProvider("demo-tenant"));

        var context = services.AddBoundedContext("default")
            .WithEventTypes(typeof(CartCreated).Assembly);

        if (useSql)
        {
            context.UseSqlServer(o =>
            {
                o.ConnectionString = connectionString!;
                o.RequireTenantId = false;
                o.EnableOutbox = true;
            });
            services.AddSingleton<ConsoleOutboxPublisher>();
            services.AddSingleton<IOutboxPublisher>(sp => sp.GetRequiredService<ConsoleOutboxPublisher>());
            services.AddSingleton<IOutboxWriter>(sp => sp.GetRequiredKeyedService<IOutboxWriter>("default"));
        }
        else
        {
            context.UseInMemory();
        }

        services.AddSingleton<DemoDataSeeder>();
        services.AddSingleton<ProjectionRunner>();
        services.AddSingleton<CartViewProjector>();
        services.AddSingleton<OrderViewProjector>();
        services.AddSingleton<ProductCatalogProjector>();
        services.AddSingleton<DcbDemoDataSeeder>();
        services.AddSingleton<DcbCartService>();
        services.AddSingleton<DcbOrderService>();
        services.AddSingleton<DcbProductService>();
        services.AddSingleton<DcbCartViewProjector>();
        services.AddSingleton<DcbOrderViewProjector>();
        services.AddSingleton<DcbProductCatalogProjector>();
        services.AddSingleton<TraditionalECommerceDemo>();
        services.AddSingleton<DcbECommerceDemo>();
        services.AddSingleton<HybridPatternDemo>();
        if (useSql)
            services.AddSingleton<OutboxDemo>();

        await using var provider = services.BuildServiceProvider();
        var hosted = new List<IHostedService>();
        try
        {
            if (useSql)
            {
                var store = provider.GetRequiredKeyedService<IEventStore>("default");
                if (store is not SqlServerEventStore sql)
                    throw new InvalidOperationException("SQL mode did not resolve SqlServerEventStore.");
                await sql.InitializeSchemaAsync();
                await provider.GetRequiredKeyedService<IOutboxWriter>("default").InitializeSchemaAsync();
            }

            if (approach == "outbox")
            {
                var demo = provider.GetRequiredService<OutboxDemo>();
                await demo.RunAsync(async () =>
                {
                    hosted.AddRange(provider.GetServices<IHostedService>());
                    foreach (var service in hosted)
                        await service.StartAsync(CancellationToken.None);
                });
                return;
            }

            if (useSql)
            {
                hosted.AddRange(provider.GetServices<IHostedService>());
                foreach (var service in hosted)
                    await service.StartAsync(CancellationToken.None);
            }

            switch (approach)
            {
                case "dcb":
                    await provider.GetRequiredService<DcbECommerceDemo>().RunAsync();
                    break;
                case "hybrid":
                    await provider.GetRequiredService<HybridPatternDemo>().RunAsync();
                    break;
                default:
                    await provider.GetRequiredService<TraditionalECommerceDemo>().RunAsync();
                    break;
            }
        }
        finally
        {
            foreach (var service in hosted.AsEnumerable().Reverse())
                await service.StopAsync(CancellationToken.None);
        }
    }

    private static void PrintUsage()
    {
        Console.WriteLine("Usage: dotnet run --project demos/LawnDart.Demo.ECommerce -- <approach> [--backend inmemory|sqlserver]");
        Console.WriteLine();
        Console.WriteLine("Approaches:");
        Console.WriteLine("  traditional   Cart, order, and product aggregates (default)");
        Console.WriteLine("  dcb           The same lifecycle with tag queries and append conditions");
        Console.WriteLine("  hybrid        Product aggregate plus tagged inventory and fulfillment");
        Console.WriteLine("  eda           Reactor, event processor, and task processor");
        Console.WriteLine("  multicontext  ordering and catalog stores in one host");
        Console.WriteLine("  outbox        SQL transactional outbox and a dead-letter reset");
        Console.WriteLine();
        Console.WriteLine("  --run-all     traditional, dcb, hybrid, eda, and multicontext.");
        Console.WriteLine("                SQL Server also runs outbox.");
        Console.WriteLine();
        Console.WriteLine("InMemory is the default. It needs no database.");
        Console.WriteLine("SQL Server needs Docker, or LAWNDART_SQL_CONNECTION / ConnectionStrings:ECommerce.");
        Console.WriteLine("A supplied connection string must already name a database. The host creates tables.");
    }
}

/// <summary>
/// Supplies one tenant id for this console.
/// </summary>
internal sealed class DemoTenantContextProvider : ITenantContextProvider
{
    private readonly string? _tenantId;

    public DemoTenantContextProvider(string? tenantId)
    {
        _tenantId = tenantId;
    }

    /// <summary>The demo tenant, or null when this host has none.</summary>
    public string? GetTenantId() => _tenantId;

    /// <summary>The demo tenant. Throws when none was configured.</summary>
    public string GetTenantIdRequired() => _tenantId
        ?? throw new InvalidOperationException("Tenant ID is not available");
}
