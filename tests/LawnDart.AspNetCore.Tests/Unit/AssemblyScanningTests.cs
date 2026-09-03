using LawnDart.AspNetCore;
using LawnDart.AspNetCore.Tests;
using Xunit;

namespace LawnDart.AspNetCore.Tests.Unit;

public class AssemblyScanningTests
{
    [Fact]
    public void DiscoverHandlers_FindsAllConcreteHandlers()
    {
        var assembly = typeof(CreateOrderCommandHandler).Assembly;
        var handlers = CommandEndpointRegistrar.DiscoverHandlers(assembly).ToList();

        Assert.Contains(typeof(CreateOrderCommandHandler), handlers);
        Assert.Contains(typeof(ShipOrderCommandHandler), handlers);
    }

    [Fact]
    public void DiscoverHandlers_ExcludesAbstractClasses()
    {
        var assembly = typeof(CreateOrderCommandHandler).Assembly;
        var handlers = CommandEndpointRegistrar.DiscoverHandlers(assembly).ToList();

        Assert.DoesNotContain(typeof(AbstractHandler<>), handlers);
    }

    [Fact]
    public void DiscoverHandlers_ExcludesInterfaces()
    {
        var assembly = typeof(CreateOrderCommandHandler).Assembly;
        var handlers = CommandEndpointRegistrar.DiscoverHandlers(assembly).ToList();

        Assert.All(handlers, h => Assert.False(h.IsInterface));
    }

    [Fact]
    public void GetCommandType_ReturnsCorrectGenericArgument()
    {
        var commandType = CommandEndpointRegistrar.GetCommandType(
            typeof(CreateOrderCommandHandler));

        Assert.Equal(typeof(CreateOrderCommand), commandType);
    }

    [Fact]
    public void GetCommandType_ReturnsNull_WhenNotAHandler()
    {
        var commandType = CommandEndpointRegistrar.GetCommandType(typeof(string));
        Assert.Null(commandType);
    }
}
