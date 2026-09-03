using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Tests.BoundedContext;

public class BoundedContextRegistryTests
{
    [Fact]
    public void Register_SingleContext_IsAvailable()
    {
        var registry = new BoundedContextRegistry();
        registry.Register("ordering");

        Assert.Single(registry.ContextNames);
        Assert.Equal("ordering", registry.ContextNames[0]);
        Assert.True(registry.Contains("ordering"));
        Assert.False(registry.IsMultiContext);
    }

    [Fact]
    public void Register_TwoContexts_IsMultiContext()
    {
        var registry = new BoundedContextRegistry();
        registry.Register("ordering");
        registry.Register("catalog");

        Assert.Equal(2, registry.ContextNames.Count);
        Assert.True(registry.IsMultiContext);
        Assert.True(registry.Contains("ordering"));
        Assert.True(registry.Contains("catalog"));
    }

    [Fact]
    public void Register_DuplicateName_ThrowsInvalidOperationException()
    {
        var registry = new BoundedContextRegistry();
        registry.Register("ordering");

        var ex = Assert.Throws<InvalidOperationException>(() => registry.Register("ordering"));
        Assert.Contains("ordering", ex.Message);
    }

    [Fact]
    public void Contains_CaseInsensitive()
    {
        var registry = new BoundedContextRegistry();
        registry.Register("Ordering");

        Assert.True(registry.Contains("ordering"));
        Assert.True(registry.Contains("ORDERING"));
    }
}
