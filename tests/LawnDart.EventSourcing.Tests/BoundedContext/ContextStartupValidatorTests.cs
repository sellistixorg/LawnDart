using Microsoft.Extensions.Logging.Abstractions;
using LawnDart.EventSourcing.Context;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Tests.BoundedContext;

public class ContextStartupValidatorTests
{
    [Fact]
    public async Task StartAsync_SingleContextNoHandlers_DoesNotThrow()
    {
        var contextRegistry = new BoundedContextRegistry();
        contextRegistry.Register("default");
        var commandRegistry = new DefaultCommandContextRegistry();

        var validator = new ContextStartupValidator(
            contextRegistry, commandRegistry,
            NullLogger<ContextStartupValidator>.Instance);

        // Should not throw
        await validator.StartAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StartAsync_MultiContext_WithHandlers_DoesNotThrow()
    {
        var contextRegistry = new BoundedContextRegistry();
        contextRegistry.Register("ordering");
        contextRegistry.Register("catalog");

        var commandRegistry = new DefaultCommandContextRegistry();
        commandRegistry.Register(typeof(StubCommandA), "ordering");
        commandRegistry.Register(typeof(StubCommandB), "catalog");

        var validator = new ContextStartupValidator(
            contextRegistry, commandRegistry,
            NullLogger<ContextStartupValidator>.Instance);

        await validator.StartAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StopAsync_AlwaysCompletesSuccessfully()
    {
        var validator = new ContextStartupValidator(
            new BoundedContextRegistry(),
            new DefaultCommandContextRegistry(),
            NullLogger<ContextStartupValidator>.Instance);

        await validator.StopAsync(CancellationToken.None);
    }

    // ── Stubs ──────────────────────────────────────────────────────────────────

    private sealed record StubCommandA(Guid Id) : ICommand;
    private sealed record StubCommandB(Guid Id) : ICommand;
}
