using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using LawnDart.EventSourcing.Context;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Tests.BoundedContext;

public class ContextStartupValidatorTests
{
    [EventTypeName("validator-stub")]
    private sealed record StubEvent(Guid Id, DateTime Timestamp) : IEvent;

    private sealed record StubCommandA(Guid Id) : ICommand;
    private sealed record StubCommandB(Guid Id) : ICommand;

    [Fact]
    public async Task StartAsync_SingleContextWithCatalog_DoesNotThrow()
    {
        var contextRegistry = new BoundedContextRegistry();
        contextRegistry.Register("default");
        var commandRegistry = new DefaultCommandContextRegistry();
        var services = new ServiceCollection();
        services.AddKeyedSingleton<IEventTypeCatalog>("default", EventTypeCatalog.Materialize([typeof(StubEvent)]));
        await using var provider = services.BuildServiceProvider();

        var validator = new ContextStartupValidator(
            contextRegistry, commandRegistry, provider,
            NullLogger<ContextStartupValidator>.Instance);

        await validator.StartAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StartAsync_ContextWithoutWithEventTypes_Throws()
    {
        var contextRegistry = new BoundedContextRegistry();
        contextRegistry.Register("default");
        var commandRegistry = new DefaultCommandContextRegistry();
        var services = new ServiceCollection();
        services.AddKeyedSingleton("default", EventStoreContextMarker.Instance);
        await using var provider = services.BuildServiceProvider();

        var validator = new ContextStartupValidator(
            contextRegistry, commandRegistry, provider,
            NullLogger<ContextStartupValidator>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => validator.StartAsync(CancellationToken.None));
        Assert.Contains("WithEventTypes", ex.Message);
        Assert.Contains("default", ex.Message);
    }

    [Fact]
    public async Task StartAsync_MultiContext_WithHandlersAndCatalogs_DoesNotThrow()
    {
        var contextRegistry = new BoundedContextRegistry();
        contextRegistry.Register("ordering");
        contextRegistry.Register("catalog");

        var commandRegistry = new DefaultCommandContextRegistry();
        commandRegistry.Register(typeof(StubCommandA), "ordering");
        commandRegistry.Register(typeof(StubCommandB), "catalog");

        var services = new ServiceCollection();
        var eventCatalog = EventTypeCatalog.Materialize([typeof(StubEvent)]);
        services.AddKeyedSingleton<IEventTypeCatalog>("ordering", eventCatalog);
        services.AddKeyedSingleton<IEventTypeCatalog>("catalog", eventCatalog);
        await using var provider = services.BuildServiceProvider();

        var validator = new ContextStartupValidator(
            contextRegistry, commandRegistry, provider,
            NullLogger<ContextStartupValidator>.Instance);

        await validator.StartAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StopAsync_AlwaysCompletesSuccessfully()
    {
        var validator = new ContextStartupValidator(
            new BoundedContextRegistry(),
            new DefaultCommandContextRegistry(),
            new ServiceCollection().BuildServiceProvider(),
            NullLogger<ContextStartupValidator>.Instance);

        await validator.StopAsync(CancellationToken.None);
    }
}
