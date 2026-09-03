using Microsoft.Extensions.DependencyInjection;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Tests.BoundedContext;

public class BoundedContextBuilderTests
{
    [Fact]
    public void AddBoundedContext_SingleContext_RegistersRegistry()
    {
        var services = new ServiceCollection();
        var builder  = services.AddBoundedContext("ordering");

        Assert.Equal("ordering", builder.ContextName);
        Assert.Same(services, builder.Services);

        var sp       = services.BuildServiceProvider();
        var registry = sp.GetRequiredService<IBoundedContextRegistry>();
        Assert.True(registry.Contains("ordering"));
        Assert.False(registry.IsMultiContext);
    }

    [Fact]
    public void AddBoundedContext_TwoContexts_BothInRegistry()
    {
        var services = new ServiceCollection();
        services.AddBoundedContext("ordering");
        services.AddBoundedContext("catalog");

        var sp       = services.BuildServiceProvider();
        var registry = sp.GetRequiredService<IBoundedContextRegistry>();

        Assert.Equal(2, registry.ContextNames.Count);
        Assert.True(registry.IsMultiContext);
        Assert.True(registry.Contains("ordering"));
        Assert.True(registry.Contains("catalog"));
    }

    [Fact]
    public void AddBoundedContext_DuplicateName_Throws()
    {
        var services = new ServiceCollection();
        services.AddBoundedContext("ordering");

        Assert.Throws<InvalidOperationException>(() => services.AddBoundedContext("ordering"));
    }

    [Fact]
    public void AddBoundedContext_NullName_Throws()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentException>(() => services.AddBoundedContext(null!));
    }

    [Fact]
    public void AddBoundedContext_EmptyName_Throws()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentException>(() => services.AddBoundedContext("   "));
    }
}
