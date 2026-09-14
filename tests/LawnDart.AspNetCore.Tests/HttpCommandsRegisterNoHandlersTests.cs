using Microsoft.Extensions.DependencyInjection;
using LawnDart;
using LawnDart.AspNetCore;
using LawnDart.EventSourcing;
using LawnDart.EventStore;

namespace LawnDart.AspNetCore.Tests;

public class HttpCommandsRegisterNoHandlersTests
{
    [Fact]
    public void AddLawnDartHttpCommands_DoesNotRegisterCommandHandlers()
    {
        var services = new ServiceCollection();
        services.AddLawnDartHttpCommands(typeof(CreateOrderCommandHandler).Assembly);

        Assert.DoesNotContain(services, IsCommandHandlerRegistration);
    }

    [Fact]
    public void DefaultContext_HttpPlusWithCommandHandlers_RegistersUnkeyedAliasOnly()
    {
        var services = new ServiceCollection();
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddBoundedContext("default")
            .UseInMemory()
            .WithCommandHandlers<CreateOrderCommandHandler>();
        services.AddLawnDartHttpCommands(typeof(CreateOrderCommandHandler).Assembly);

        var unkeyed = services.Where(d =>
            d.ServiceType == typeof(ICommandHandler<CreateOrderCommand>) &&
            d.ServiceKey is null).ToList();

        var single = Assert.Single(unkeyed);
        Assert.NotNull(single.ImplementationFactory);
        Assert.Null(single.ImplementationType);
        Assert.Equal(ServiceLifetime.Transient, single.Lifetime);
    }

    private static bool IsCommandHandlerRegistration(ServiceDescriptor d)
    {
        var type = d.ServiceType;
        return type.IsGenericType &&
               type.GetGenericTypeDefinition() == typeof(ICommandHandler<>);
    }
}
