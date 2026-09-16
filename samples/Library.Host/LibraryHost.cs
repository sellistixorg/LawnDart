using LawnDart;
using LawnDart.AspNetCore;
using LawnDart.Authorization.AspNetCore;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Lightweight;
using Library.Domain;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Library.Host;

public static class LibraryHost
{
    public static IServiceCollection AddInMemoryLibrary(IServiceCollection services)
    {
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddSingleton<ITenantContextProvider, AmbientTenantContextProvider>();
        services.AddInMemoryProjectionStores("default");
        var ctx = services.AddBoundedContext("default");
        ctx.UseInMemory();
        ctx.WithCommandHandlers<BorrowBookHandler>();
        ctx.WithEventTypes<BookAdded>();
        ctx.WithProjections(
            [typeof(LibraryCatalogProjection).Assembly],
            opts =>
            {
                opts.PollInterval = TimeSpan.FromMilliseconds(200);
                opts.CheckpointInterval = 100;
            });
        return services;
    }

    public static IServiceCollection AddSqlLibrary(IServiceCollection services, string connectionString)
    {
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddSingleton<ITenantContextProvider, AmbientTenantContextProvider>();
        var ctx = services.AddBoundedContext("default");
        ctx.UseSqlServer(o =>
        {
            o.ConnectionString = connectionString;
            o.RequireTenantId = false;
        });
        ctx.WithCommandHandlers<BorrowBookHandler>();
        ctx.WithEventTypes<BookAdded>();
        return services;
    }

    public static void AddTwoContexts(IServiceCollection services)
    {
        var library = services.AddBoundedContext("library");
        library.UseInMemory();
        library.WithCommandHandlers<BorrowBookHandler>();
        library.WithEventTypes<BookAdded>();

        var other = services.AddBoundedContext("other");
        other.UseInMemory();
    }

    public static void MapLibraryHttp(IServiceCollection services, WebApplication app)
    {
        services.AddLawnDartHttpCommands(typeof(BorrowBookHandler).Assembly);
        services.AddHttpAuthorizationContext();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapLawnDartCommands();
        app.MapProjectionQueries("default");
    }
}
