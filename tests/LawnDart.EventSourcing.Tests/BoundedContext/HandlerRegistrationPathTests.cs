using Microsoft.Extensions.DependencyInjection;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.TestUtilities;

namespace LawnDart.EventSourcing.Tests.BoundedContext;

public class HandlerRegistrationPathTests
{
    [Fact]
    public async Task DefaultContext_ResolvesUnkeyedHandlerBuiltThroughContextServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });

        services.AddBoundedContext("default")
            .UseInMemory()
            .WithCommandHandlers([typeof(WithCommandHandlersAndDispatcherTests.RepositoryCapturingHandler)]);

        var unkeyed = services.Single(d =>
            d.ServiceType == typeof(ICommandHandler<WithCommandHandlersAndDispatcherTests.CheckRepositoryCommand>) &&
            d.ServiceKey is null);
        Assert.NotNull(unkeyed.ImplementationFactory);
        Assert.Null(unkeyed.ImplementationType);

        var sp = services.BuildServiceProvider();
        WithCommandHandlersAndDispatcherTests.RepositoryCapturingHandler.CapturedRepository = null;

        var handler = sp.GetRequiredService<ICommandHandler<WithCommandHandlersAndDispatcherTests.CheckRepositoryCommand>>();
        await handler.HandleAsync(new WithCommandHandlersAndDispatcherTests.CheckRepositoryCommand(Guid.NewGuid()));

        Assert.NotNull(WithCommandHandlersAndDispatcherTests.RepositoryCapturingHandler.CapturedRepository);
    }

    [Fact]
    public void NamedContext_HasNoUnkeyedHandlerAlias()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });

        services.AddBoundedContext("ordering")
            .UseInMemory()
            .WithCommandHandlers([typeof(WithCommandHandlersAndDispatcherTests.CreateOrderHandler)]);

        var sp = services.BuildServiceProvider();

        Assert.Null(sp.GetService<ICommandHandler<WithCommandHandlersAndDispatcherTests.CreateOrderCommand>>());
        Assert.NotNull(sp.GetRequiredKeyedService<ICommandHandler<WithCommandHandlersAndDispatcherTests.CreateOrderCommand>>("ordering"));
    }

    [Fact]
    public void MarkerOverload_RegistersHandlersFromMarkerAssembly()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });

        services.AddBoundedContext("default")
            .UseInMemory()
            .WithCommandHandlers<WithCommandHandlersAndDispatcherTests.CreateOrderHandler>();

        var sp = services.BuildServiceProvider();
        Assert.NotNull(sp.GetRequiredService<ICommandHandler<WithCommandHandlersAndDispatcherTests.CreateOrderCommand>>());
    }

    [Fact]
    public void ZeroHandlerScan_NamesAssembly()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("default");

        var ex = Assert.Throws<InvalidOperationException>(
            () => builder.WithCommandHandlers(typeof(object).Assembly));

        Assert.Contains("WithCommandHandlers scanned assembly", ex.Message);
        Assert.Contains(typeof(object).Assembly.GetName().Name!, ex.Message);
        Assert.Contains("ICommandHandler<>", ex.Message);
        Assert.DoesNotContain("GetCallingAssembly", ex.Message);
    }

    [Fact]
    public void ZeroHandlerScan_CallingAssemblyFallback_NamesFallback()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("default");

        var ex = Assert.Throws<InvalidOperationException>(
            () => builder.WithCommandHandlersUsingCallingAssembly());

        Assert.Contains("LawnDart.EventSourcing", ex.Message);
        Assert.Contains("GetCallingAssembly", ex.Message);
    }

    [Fact]
    public void ZeroEventScan_NamesAssembly()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("default");

        var ex = Assert.Throws<InvalidOperationException>(
            () => builder.WithEventTypes(typeof(object).Assembly));

        Assert.Contains("WithEventTypes scanned assembly", ex.Message);
        Assert.Contains(typeof(object).Assembly.GetName().Name!, ex.Message);
        Assert.Contains("IEvent", ex.Message);
        Assert.DoesNotContain("GetCallingAssembly", ex.Message);
    }

    [Fact]
    public void ZeroEventScan_CallingAssemblyFallback_NamesFallback()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("default");

        var ex = Assert.Throws<InvalidOperationException>(
            () => builder.WithEventTypesUsingCallingAssembly());

        Assert.Contains("LawnDart", ex.Message);
        Assert.Contains("GetCallingAssembly", ex.Message);
    }

    [Fact]
    public void MarkerEventTypes_ScansMarkerAssembly()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("default");

        var ex = Assert.Throws<InvalidOperationException>(
            () => builder.WithEventTypes<LawnDartOptions>());

        Assert.Contains("LawnDart", ex.Message);
        Assert.Contains("IEvent", ex.Message);
        Assert.DoesNotContain("GetCallingAssembly", ex.Message);
    }
}
