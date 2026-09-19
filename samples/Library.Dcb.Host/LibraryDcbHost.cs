using LawnDart;
using LawnDart.EventStore;
using LawnDart.EventSourcing;
using Library.Dcb.Domain;
using Microsoft.Extensions.DependencyInjection;

namespace Library.Dcb.Host;

public static class LibraryDcbHost
{
    public static IServiceCollection AddDcbLibrary(IServiceCollection services)
    {
        services.AddLawnDart(o => o.RequireTenantId = false);
        var ctx = services.AddBoundedContext("default");
        ctx.UseInMemory();
        ctx.WithCommandHandlers<BorrowBookHandler>();
        ctx.WithEventTypes<BookAdded>();
        return services;
    }
}
