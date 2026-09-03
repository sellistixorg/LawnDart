using Microsoft.Extensions.DependencyInjection;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.SqlServer.Tests.MultiContext;

/// <summary>
/// Verifies that two <c>AddBoundedContext().UseSqlServer()</c> calls register independent
/// keyed <see cref="IEventStore"/> instances without requiring a live SQL Server.
/// Options eagerly validation (missing ConnectionString) is also covered here.
/// </summary>
public class SqlServerMultiContextRegistrationTests
{
    private const string FakeConnectionString =
        "Server=localhost;Database=TestDb;User Id=sa;Password=Test123!;TrustServerCertificate=True;";

    [Fact]
    public void UseSqlServer_RegistersKeyedEventStore_ForContext()
    {
        var services = new ServiceCollection();

        services.AddBoundedContext("ordering")
                .UseSqlServer(opt =>
                {
                    opt.ConnectionString = FakeConnectionString;
                    opt.SchemaName       = "ordering";
                });

        var sp = services.BuildServiceProvider();

        // The keyed store should be resolvable (even if SQL Server is unreachable,
        // the factory lambda is only called on first resolution).
        var store = sp.GetRequiredKeyedService<IEventStore>("ordering");
        Assert.NotNull(store);
    }

    [Fact]
    public void TwoContexts_RegisterIndependentKeyedEventStores()
    {
        var services = new ServiceCollection();

        services.AddBoundedContext("ordering")
                .UseSqlServer(opt =>
                {
                    opt.ConnectionString = FakeConnectionString;
                    opt.SchemaName       = "ordering";
                });

        services.AddBoundedContext("catalog")
                .UseSqlServer(opt =>
                {
                    opt.ConnectionString = FakeConnectionString;
                    opt.SchemaName       = "catalog";
                });

        var sp = services.BuildServiceProvider();

        var orderingStore = sp.GetRequiredKeyedService<IEventStore>("ordering");
        var catalogStore  = sp.GetRequiredKeyedService<IEventStore>("catalog");

        Assert.NotNull(orderingStore);
        Assert.NotNull(catalogStore);
        Assert.NotSame(orderingStore, catalogStore);
    }

    [Fact]
    public void TwoContexts_RegisterIndependentBoundedContextEventStores()
    {
        var services = new ServiceCollection();

        services.AddBoundedContext("ordering")
                .UseSqlServer(opt => { opt.ConnectionString = FakeConnectionString; opt.SchemaName = "ordering"; });
        services.AddBoundedContext("catalog")
                .UseSqlServer(opt => { opt.ConnectionString = FakeConnectionString; opt.SchemaName = "catalog"; });

        var sp       = services.BuildServiceProvider();
        var contexts = sp.GetServices<IBoundedContextEventStore>().ToList();

        Assert.Equal(2, contexts.Count);
        Assert.Contains(contexts, c => c.ContextName == "ordering");
        Assert.Contains(contexts, c => c.ContextName == "catalog");
    }

    [Fact]
    public void TwoContexts_BoundedContextRegistry_TracksBothNames()
    {
        var services = new ServiceCollection();

        services.AddBoundedContext("ordering")
                .UseSqlServer(opt => { opt.ConnectionString = FakeConnectionString; opt.SchemaName = "ordering"; });
        services.AddBoundedContext("catalog")
                .UseSqlServer(opt => { opt.ConnectionString = FakeConnectionString; opt.SchemaName = "catalog"; });

        var sp       = services.BuildServiceProvider();
        var registry = sp.GetRequiredService<IBoundedContextRegistry>();

        Assert.True(registry.IsMultiContext);
        Assert.True(registry.Contains("ordering"));
        Assert.True(registry.Contains("catalog"));
    }

    [Fact]
    public void UseSqlServer_ThrowsArgumentException_WhenConnectionStringMissing()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() =>
            services.AddBoundedContext("ordering")
                    .UseSqlServer(opt => { /* no ConnectionString */ }));
    }

    [Fact]
    public void TwoContexts_DifferentSchemaNames_DoNotConflict()
    {
        var services = new ServiceCollection();

        // Same database but different schemas
        services.AddBoundedContext("ordering")
                .UseSqlServer(opt => { opt.ConnectionString = FakeConnectionString; opt.SchemaName = "ordering"; });
        services.AddBoundedContext("context2")
                .UseSqlServer(opt => { opt.ConnectionString = FakeConnectionString; opt.SchemaName = "context2"; });

        // Building the provider should not throw
        var sp = services.BuildServiceProvider();
        Assert.NotNull(sp);
    }
}
