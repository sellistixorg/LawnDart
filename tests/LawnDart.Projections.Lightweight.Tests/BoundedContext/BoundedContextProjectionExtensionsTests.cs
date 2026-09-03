using Microsoft.Extensions.DependencyInjection;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.BoundedContext;

/// <summary>
/// Tests for <c>AddInMemoryProjectionStores(contextName)</c> and
/// <see cref="LightweightProjectionOptions.ContextName"/>.
/// </summary>
public class BoundedContextProjectionExtensionsTests
{
    private static IServiceCollection BaseServices()
    {
        var services = new ServiceCollection();
        return services;
    }

    [Fact]
    public void AddInMemoryProjectionStores_WithContextName_RegistersKeyedViewStore()
    {
        var services = BaseServices();
        services.AddInMemoryProjectionStores("ordering");

        var sp        = services.BuildServiceProvider();
        var viewStore = sp.GetRequiredKeyedService<IViewStore>("ordering");

        Assert.NotNull(viewStore);
        Assert.IsType<InMemoryViewStore>(viewStore);
    }

    [Fact]
    public void AddInMemoryProjectionStores_WithContextName_RegistersKeyedCheckpointStore()
    {
        var services = BaseServices();
        services.AddInMemoryProjectionStores("ordering");

        var sp             = services.BuildServiceProvider();
        var checkpointStore = sp.GetRequiredKeyedService<ICheckpointStore>("ordering");

        Assert.NotNull(checkpointStore);
        Assert.IsType<InMemoryCheckpointStore>(checkpointStore);
    }

    [Fact]
    public void AddInMemoryProjectionStores_TwoContexts_HaveIndependentStores()
    {
        var services = BaseServices();
        services.AddInMemoryProjectionStores("ordering");
        services.AddInMemoryProjectionStores("catalog");

        var sp           = services.BuildServiceProvider();
        var orderingView = sp.GetRequiredKeyedService<IViewStore>("ordering");
        var catalogView  = sp.GetRequiredKeyedService<IViewStore>("catalog");

        Assert.NotSame(orderingView, catalogView);
    }

    [Fact]
    public void AddInMemoryProjectionStores_NullContextName_Throws()
    {
        var services = BaseServices();
        Assert.Throws<ArgumentException>(() => services.AddInMemoryProjectionStores(null!));
    }

    [Fact]
    public void LightweightProjectionOptions_ContextName_IsNullByDefault()
    {
        var options = new LightweightProjectionOptions();
        Assert.Null(options.ContextName);
    }

    [Fact]
    public void LightweightProjectionOptions_ContextName_CanBeSet()
    {
        var options = new LightweightProjectionOptions { ContextName = "ordering" };
        Assert.Equal("ordering", options.ContextName);
    }

    [Fact]
    public void AddSqlProjectionStores_WithContextName_RegistersKeyedSqlStores()
    {
        var services = BaseServices();
        services.AddSqlProjectionStores("ordering", "Server=.;Database=test;Trusted_Connection=true");

        var sp = services.BuildServiceProvider();
        var viewStore = sp.GetRequiredKeyedService<IViewStore>("ordering");
        var checkpointStore = sp.GetRequiredKeyedService<ICheckpointStore>("ordering");

        Assert.IsType<SqlViewStore>(viewStore);
        Assert.IsType<SqlCheckpointStore>(checkpointStore);
    }

    [Fact]
    public void AddSqlProjectionStores_TwoContexts_HaveIndependentStores()
    {
        var services = BaseServices();
        const string cs = "Server=.;Database=test;Trusted_Connection=true";
        services.AddSqlProjectionStores("listing", cs);
        services.AddSqlProjectionStores("inbound", cs);

        var sp = services.BuildServiceProvider();
        var listingView = sp.GetRequiredKeyedService<IViewStore>("listing");
        var inboundView = sp.GetRequiredKeyedService<IViewStore>("inbound");

        Assert.NotSame(listingView, inboundView);
        Assert.IsType<SqlViewStore>(listingView);
        Assert.IsType<SqlViewStore>(inboundView);
    }

    [Fact]
    public void AddSqlProjectionStores_NullContextName_Throws()
    {
        var services = BaseServices();
        Assert.Throws<ArgumentException>(() =>
            services.AddSqlProjectionStores(null!, "Server=.;Database=test;Trusted_Connection=true"));
    }

    [Fact]
    public void AddSqlProjectionStores_NullConnectionString_Throws()
    {
        var services = BaseServices();
        Assert.Throws<ArgumentException>(() => services.AddSqlProjectionStores("ordering", null!));
    }

    [Fact]
    public async Task InitializeSqlProjectionStoresAsync_WithInMemoryStores_Throws()
    {
        var services = BaseServices();
        services.AddInMemoryProjectionStores("ordering");
        var sp = services.BuildServiceProvider();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            sp.InitializeSqlProjectionStoresAsync("ordering"));
    }
}
