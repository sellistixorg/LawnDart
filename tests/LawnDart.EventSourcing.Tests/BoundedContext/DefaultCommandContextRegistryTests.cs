namespace LawnDart.EventSourcing.Tests.BoundedContext;

public class DefaultCommandContextRegistryTests
{
    [Fact]
    public void Register_And_GetContextName_ReturnsCorrectContext()
    {
        var registry = new DefaultCommandContextRegistry();
        registry.Register(typeof(TestCommandA), "ordering");

        Assert.Equal("ordering", registry.GetContextName(typeof(TestCommandA)));
    }

    [Fact]
    public void Register_TwoCommandsInDifferentContexts_BothResolvable()
    {
        var registry = new DefaultCommandContextRegistry();
        registry.Register(typeof(TestCommandA), "ordering");
        registry.Register(typeof(TestCommandB), "catalog");

        Assert.Equal("ordering", registry.GetContextName(typeof(TestCommandA)));
        Assert.Equal("catalog",  registry.GetContextName(typeof(TestCommandB)));
    }

    [Fact]
    public void Register_SameCommandSameContext_IsIdempotent()
    {
        var registry = new DefaultCommandContextRegistry();
        registry.Register(typeof(TestCommandA), "ordering");
        registry.Register(typeof(TestCommandA), "ordering"); // second registration same context — no throw

        Assert.Equal("ordering", registry.GetContextName(typeof(TestCommandA)));
    }

    [Fact]
    public void Register_SameCommandDifferentContext_Throws()
    {
        var registry = new DefaultCommandContextRegistry();
        registry.Register(typeof(TestCommandA), "ordering");

        var ex = Assert.Throws<InvalidOperationException>(
            () => registry.Register(typeof(TestCommandA), "catalog"));

        Assert.Contains("TestCommandA", ex.Message);
        Assert.Contains("ordering", ex.Message);
    }

    [Fact]
    public void GetContextName_UnregisteredCommand_Throws()
    {
        var registry = new DefaultCommandContextRegistry();

        var ex = Assert.Throws<InvalidOperationException>(
            () => registry.GetContextName(typeof(TestCommandA)));

        Assert.Contains("TestCommandA", ex.Message);
    }

    [Fact]
    public void IsRegistered_ReturnsTrueAfterRegistration()
    {
        var registry = new DefaultCommandContextRegistry();
        Assert.False(registry.IsRegistered(typeof(TestCommandA)));

        registry.Register(typeof(TestCommandA), "ordering");
        Assert.True(registry.IsRegistered(typeof(TestCommandA)));
    }

    [Fact]
    public void GetAllRegistrations_GroupsCorrectly()
    {
        var registry = new DefaultCommandContextRegistry();
        registry.Register(typeof(TestCommandA), "ordering");
        registry.Register(typeof(TestCommandB), "ordering");
        registry.Register(typeof(TestCommandC), "catalog");

        var all = registry.GetAllRegistrations();

        Assert.Equal(2, all.Count);
        Assert.Equal(2, all["ordering"].Count);
        Assert.Single(all["catalog"]);
    }

    // ── Test command stubs ─────────────────────────────────────────────────────

    private sealed record TestCommandA(Guid Id) : ICommand;
    private sealed record TestCommandB(Guid Id) : ICommand;
    private sealed record TestCommandC(Guid Id) : ICommand;
}
