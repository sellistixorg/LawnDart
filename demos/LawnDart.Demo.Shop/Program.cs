using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.Extensions.DependencyInjection.Extensions;
using LawnDart;
using LawnDart.AspNetCore;
using LawnDart.Authorization;
using LawnDart.Demo.Shop.Authorization;
using LawnDart.Demo.Shop.Domain.Product;
using LawnDart.Demo.Shop.Domain.Order.Commands;
using LawnDart.Demo.Shop.Domain.Order.Events;
using LawnDart.Demo.Shop.Domain.Product.Commands;
using LawnDart.Demo.Shop.EDA;
using LawnDart.Demo.Shop.Handlers;
using LawnDart.Demo.Shop.Infrastructure;
using LawnDart.Demo.Shop.Inventory;
using LawnDart.Demo.Shop.Projections;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;
using LawnDart.Messaging.Outbox;
using LawnDart.Messaging.SqlServer;
using LawnDart.Metadata;
using LawnDart.Outbox;
using LawnDart.Projections.Lightweight;
using LawnDart.Snapshots;

// DEMO ONLY: cookie users below have no passwords. Do not copy them into a real app.

var builder = WebApplication.CreateBuilder(args);

var smoke = args.Contains("--smoke");
if (smoke)
{
    builder.WebHost.UseUrls("http://127.0.0.1:5096");
    builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["Shop:OverdueOrderThresholdSeconds"] = "3600"
    });
}

var useSql = args.Contains("--sql")
    || builder.Configuration.GetValue("EventStore:UseSqlServer", false);

builder.Services.AddLawnDart(opts =>
{
    opts.RequireTenantId = false;
    opts.EnableAuthorization = true;
});

builder.Services.AddSingleton<CommandEventLog>();
builder.Services.AddHttpContextAccessor();
builder.Services.AddMetadataProvider<HttpContextMetadataProvider>();
builder.Services.AddTenantContextProvider<DefaultTenantContextProvider>();
builder.Services.AddSingleton(new ShopRuntimeOptions { PublishDirectly = !useSql });

var shop = builder.Services.AddBoundedContext("default")
    .WithEventTypes<OrderPlaced>()
    .WithCommandHandlers<CreateProductCommandHandler>()
    .WithTagProvider<ShopTagProvider>();

if (useSql)
{
    var connectionString =
        builder.Configuration.GetConnectionString("Shop")
        ?? Environment.GetEnvironmentVariable("LAWNDART_SQL_CONNECTION")
        ?? throw new InvalidOperationException(
            "SQL mode requires ConnectionStrings:Shop or LAWNDART_SQL_CONNECTION.");

    builder.Services.AddSqlProjectionStores("default", connectionString, schemaName: "dbo");
    builder.Services.AddInMemoryMessaging();
    builder.Services.AddMessageTransportOutboxPublisher();
    builder.Services.AddSqlInboxStore(connectionString);
    shop.UseSqlServer(opts =>
        {
            opts.ConnectionString = connectionString;
            opts.RequireTenantId = false;
            opts.EnableOutbox = true;
        })
        .WithSnapshots(cfg =>
        {
            cfg.RegisterForDcb<InventoryEntity>(new EventCountSnapshotStrategy(5));
            cfg.RegisterForDcb<SkuRegistryEntity>(new EventCountSnapshotStrategy(5));
        })
        .WithProjections(
            [typeof(ProductCatalogProjection).Assembly],
            opts =>
            {
                opts.PollInterval = TimeSpan.FromMilliseconds(200);
                opts.CheckpointInterval = 50;
            });
}
else
{
    var snapshots = new SnapshotStrategyResolver();
    snapshots.RegisterForDcb<InventoryEntity>(new EventCountSnapshotStrategy(5));
    snapshots.RegisterForDcb<SkuRegistryEntity>(new EventCountSnapshotStrategy(5));
    builder.Services.AddSingleton<ISnapshotStrategyResolver>(snapshots);

    builder.Services.AddInMemoryProjectionStores("default");
    builder.Services.AddInMemoryMessaging();
    shop.UseInMemory()
        .WithProjections(
            [typeof(ProductCatalogProjection).Assembly],
            opts =>
            {
                opts.PollInterval = TimeSpan.FromMilliseconds(200);
                opts.CheckpointInterval = 50;
            });
}

DecorateDefaultEventStore(builder.Services);

builder.Services.AddSingleton<IAuthorizationProvider, ShopAuthorizationProvider>();
builder.Services.AddScoped<AuthorizationService>();
builder.Services.AddScoped<IAuthorizationContextProvider, ShopAuthorizationContextProvider>();

builder.Services
    .AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
    .AddCookie(opts =>
    {
        opts.LoginPath = "/login";
        opts.LogoutPath = "/logout";
    });
builder.Services.AddAuthorization();
builder.Services.AddCascadingAuthenticationState();

builder.Services.AddLawnDartHttpCommands(typeof(Program).Assembly);

builder.Services.AddProjectionDebugServices("default");

builder.Services.AddReactor<OrderReactor, OrderPlaced>();
builder.Services.AddReactor<FulfillmentReactor, OrderPaymentProcessed>();

var overdueThreshold = TimeSpan.FromSeconds(
    builder.Configuration.GetValue("Shop:OverdueOrderThresholdSeconds", 30));
builder.Services.AddTaskProcessor<OverdueOrderProcessor>(opts =>
    opts.PollingInterval = TimeSpan.FromSeconds(10));
builder.Services.AddTransient<OverdueOrderProcessor>(sp =>
    new OverdueOrderProcessor(
        sp.GetRequiredKeyedService<LawnDart.Projections.Storage.IViewStore>("default"),
        timeProvider: null,
        overdueThreshold));

builder.Services.AddSingleton<ICommandDispatcher, ShopCommandDispatcher>();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddScoped<CookieForwardingHandler>();
builder.Services.AddScoped(sp =>
{
    var nav = sp.GetRequiredService<Microsoft.AspNetCore.Components.NavigationManager>();
    var handler = sp.GetRequiredService<CookieForwardingHandler>();
    handler.InnerHandler = new HttpClientHandler();
    return new HttpClient(handler) { BaseAddress = new Uri(nav.BaseUri) };
});

var app = builder.Build();

if (useSql)
    await InitializeSqlAsync(app.Services).ConfigureAwait(false);

app.UseStaticFiles();
app.UseRouting();
app.UseAuthentication();
app.UseAuthorization();
app.UseAntiforgery();
app.UseMiddleware<CorrelationMiddleware>();
app.UseMiddleware<CommandLoggingMiddleware>();

app.MapGet("/health", () => Results.Ok(new { status = "healthy", store = useSql ? "sqlserver" : "inmemory" }))
    .AllowAnonymous();

// DEMO ONLY: named users and tenants. No password.
app.MapGet("/auth/login", async (string user, HttpContext ctx) =>
{
    var (name, role, tenantId) = user.ToLowerInvariant() switch
    {
        "alice" => ("alice", "Customer", "buyers"),
        "maria" => ("maria", "Customer", "buyers"),
        "bob" => ("bob", "Seller", "bobs-store"),
        "larry" => ("larry", "Seller", "larrys-emporium"),
        "carol" => ("carol", "Warehouse", "system"),
        _ => ((string?)null, (string?)null, (string?)null)
    };

    if (name is null)
        return Results.BadRequest("Unknown user");

    var claims = new[]
    {
        new Claim(ClaimTypes.NameIdentifier, name),
        new Claim(ClaimTypes.Name, name),
        new Claim(ClaimTypes.Role, role!),
        new Claim("tenant_id", tenantId!)
    };

    var identity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
    await ctx.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, new ClaimsPrincipal(identity))
        .ConfigureAwait(false);
    return Results.Redirect("/");
});

app.MapPost("/logout", async (HttpContext ctx) =>
{
    await ctx.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme).ConfigureAwait(false);
    return Results.Redirect("/login");
});

app.MapLawnDartCommands();
app.MapProjectionQueries("default");
app.MapProjectionDebugApi(opts =>
{
    opts.RoutePrefix = "/internal/projections/debug";
    opts.RequireAuthentication = true;
});

app.MapGet("/api/debug/events", async (IEventStore store, CancellationToken ct) =>
{
    var events = new List<object>();
    await foreach (var sequenced in store.ReadByQueryStreamAsync(Query.All(), fromSequencePosition: 0, cancellationToken: ct)
        .ConfigureAwait(false))
    {
        events.Add(new
        {
            sequenced.SequencePosition,
            sequenced.StreamId,
            EventType = sequenced.Event?.GetType().Name ?? "null"
        });
    }

    return Results.Ok(new { Count = events.Count, Events = events });
}).AllowAnonymous();

app.MapRazorComponents<LawnDart.Demo.Shop.Web.App>()
    .AddInteractiveServerRenderMode();

if (smoke)
{
    await app.StartAsync().ConfigureAwait(false);
    var code = await LawnDart.Demo.Shop.ShopSmoke.RunAsync("http://127.0.0.1:5096").ConfigureAwait(false);
    await app.StopAsync().ConfigureAwait(false);
    Environment.Exit(code);
}

app.Run();

static async Task InitializeSqlAsync(IServiceProvider services)
{
    var store = services.GetRequiredKeyedService<IEventStore>("default");
    var sql = UnwrapSql(store)
        ?? throw new InvalidOperationException(
            "SQL mode did not resolve SqlServerEventStore for context 'default'.");

    await sql.InitializeSchemaAsync().ConfigureAwait(false);
    await services.GetRequiredKeyedService<IOutboxWriter>("default")
        .InitializeSchemaAsync()
        .ConfigureAwait(false);
    await services.InitializeSqlProjectionStoresAsync("default").ConfigureAwait(false);
    await services.InitializeSqlInboxStoreAsync().ConfigureAwait(false);
}

static SqlServerEventStore? UnwrapSql(IEventStore store)
{
    if (store is SqlServerEventStore sql)
        return sql;
    if (store is LoggingEventStore logging)
        return UnwrapSql(logging.Inner);
    return null;
}

static void DecorateDefaultEventStore(IServiceCollection services)
{
    ServiceDescriptor? keyed = null;
    for (var i = services.Count - 1; i >= 0; i--)
    {
        var descriptor = services[i];
        if (descriptor.ServiceType == typeof(IEventStore) && descriptor.ServiceKey is "default")
        {
            keyed = descriptor;
            services.RemoveAt(i);
            break;
        }
    }

    if (keyed?.KeyedImplementationFactory is not { } factory)
    {
        throw new InvalidOperationException(
            "The default event store must be registered before it can be decorated.");
    }

    services.AddKeyedSingleton<IEventStore>("default", (sp, key) =>
    {
        var inner = (IEventStore)factory(sp, key);
        return new LoggingEventStore(inner, sp.GetRequiredService<CommandEventLog>());
    });
}
