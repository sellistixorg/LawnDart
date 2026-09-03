using Microsoft.Extensions.DependencyInjection;
using LawnDart.Aggregates;
using LawnDart.EventStore;
using LawnDart.EventSourcing.Context;
using LawnDart.Messaging;
using LawnDart.Metadata;
using LawnDart.TestUtilities;

namespace LawnDart.EventSourcing.Tests.BoundedContext;

/// <summary>
/// Tests for <c>WithCommandHandlers()</c> scanning, DI registration, and
/// <see cref="ContextAwareCommandDispatcher"/> routing.
/// </summary>
public class WithCommandHandlersAndDispatcherTests
{
    private static IServiceProvider BuildTwoContextProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });

        // Each context gets its own explicit handler types to avoid assembly-scan conflicts
        services.AddBoundedContext("ordering")
            .UseInMemory()
            .WithCommandHandlers([typeof(CreateOrderHandler), typeof(RepositoryCapturingHandler)]);

        services.AddBoundedContext("catalog")
            .UseInMemory()
            .WithCommandHandlers([typeof(PublishProductHandler)]);

        return services.BuildServiceProvider();
    }

    [Fact]
    public void WithCommandHandlers_PopulatesCommandContextRegistry()
    {
        var sp       = BuildTwoContextProvider();
        var registry = sp.GetRequiredService<ICommandContextRegistry>();

        Assert.Equal("ordering", registry.GetContextName(typeof(CreateOrderCommand)));
        Assert.Equal("catalog",  registry.GetContextName(typeof(PublishProductCommand)));
    }

    [Fact]
    public async Task ContextAwareCommandDispatcher_RoutesOrderCommandToOrderingContext()
    {
        var sp         = BuildTwoContextProvider();
        var dispatcher = sp.GetRequiredService<ICommandDispatcher>();

        CreateOrderHandler.LastReceivedCommand = null;

        var cmd = new CreateOrderCommand(Guid.NewGuid());
        await dispatcher.DispatchAsync(cmd, new MessageContext(), CancellationToken.None);

        Assert.Equal(cmd.Id, CreateOrderHandler.LastReceivedCommand?.Id);
    }

    [Fact]
    public async Task ContextAwareCommandDispatcher_RoutesCatalogCommandToCatalogContext()
    {
        var sp         = BuildTwoContextProvider();
        var dispatcher = sp.GetRequiredService<ICommandDispatcher>();

        PublishProductHandler.LastReceivedCommand = null;

        var cmd = new PublishProductCommand(Guid.NewGuid());
        await dispatcher.DispatchAsync(cmd, new MessageContext(), CancellationToken.None);

        Assert.Equal(cmd.Id, PublishProductHandler.LastReceivedCommand?.Id);
    }

    [Fact]
    public async Task ContextAwareCommandDispatcher_HandlerReceivesContextKeyedRepository()
    {
        var sp         = BuildTwoContextProvider();
        var dispatcher = sp.GetRequiredService<ICommandDispatcher>();

        RepositoryCapturingHandler.CapturedRepository = null;

        var cmd = new CheckRepositoryCommand(Guid.NewGuid());
        await dispatcher.DispatchAsync(cmd, new MessageContext(), CancellationToken.None);

        Assert.NotNull(RepositoryCapturingHandler.CapturedRepository);
    }

    [Fact]
    public async Task ContextAwareCommandDispatcher_UnregisteredCommand_Throws()
    {
        var sp         = BuildTwoContextProvider();
        var dispatcher = sp.GetRequiredService<ICommandDispatcher>();

        var cmd = new UnregisteredCommand(Guid.NewGuid());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => dispatcher.DispatchAsync(cmd, new MessageContext(), CancellationToken.None));
    }

    // ── Commands ───────────────────────────────────────────────────────────────

    public sealed record CreateOrderCommand(Guid Id) : ICommand;
    public sealed record PublishProductCommand(Guid Id) : ICommand;
    public sealed record CheckRepositoryCommand(Guid Id) : ICommand;
    public sealed record UnregisteredCommand(Guid Id) : ICommand;

    // ── Handlers ──────────────────────────────────────────────────────────────

    /// <summary>Registered to "ordering" context via explicit type list.</summary>
    public sealed class CreateOrderHandler : ICommandHandler<CreateOrderCommand>
    {
        public static CreateOrderCommand? LastReceivedCommand;
        public Task HandleAsync(CreateOrderCommand command, CancellationToken _ = default)
        {
            LastReceivedCommand = command;
            return Task.CompletedTask;
        }
    }

    /// <summary>Registered to "catalog" context via explicit type list.</summary>
    public sealed class PublishProductHandler : ICommandHandler<PublishProductCommand>
    {
        public static PublishProductCommand? LastReceivedCommand;
        public Task HandleAsync(PublishProductCommand command, CancellationToken _ = default)
        {
            LastReceivedCommand = command;
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Injects <see cref="IAggregateRepository"/> to verify that the context-aware DI
    /// transparently provides the correct keyed repository without the handler knowing
    /// its context name.
    /// </summary>
    public sealed class RepositoryCapturingHandler : ICommandHandler<CheckRepositoryCommand>
    {
        public static IAggregateRepository? CapturedRepository;
        private readonly IAggregateRepository _repository;

        public RepositoryCapturingHandler(IAggregateRepository repository)
            => _repository = repository;

        public Task HandleAsync(CheckRepositoryCommand command, CancellationToken _ = default)
        {
            CapturedRepository = _repository;
            return Task.CompletedTask;
        }
    }

    // ── Stubs ──────────────────────────────────────────────────────────────────
}
