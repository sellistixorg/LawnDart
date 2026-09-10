using Microsoft.Extensions.DependencyInjection;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventSourcing.Context;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Metadata;
using LawnDart.TestUtilities;

namespace LawnDart.EventSourcing.Tests.BoundedContext;

public class DispatcherMessageContextTests
{
    [Fact]
    public async Task DispatchAsync_PublishesMessageContextForCapture()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });
        services.AddBoundedContext("ordering")
            .UseInMemory()
            .WithCommandHandlers([typeof(CaptureHandler)]);

        var sp = services.BuildServiceProvider();
        var dispatcher = sp.GetRequiredService<ICommandDispatcher>();
        CaptureHandler.Last = null;

        var inbound = new MessageContext
        {
            CorrelationId = "disp-corr",
            CausationId = "disp-cause",
            TenantId = "shop",
            UserId = "pat",
            Headers = new Dictionary<string, string>
            {
                ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            }
        };

        await dispatcher.DispatchAsync(new CaptureCommand(Guid.NewGuid()), inbound);

        Assert.NotNull(CaptureHandler.Last);
        Assert.Equal("disp-corr", CaptureHandler.Last!.CorrelationId);
        Assert.Equal("disp-cause", CaptureHandler.Last.CausationId);
        Assert.Equal("shop", CaptureHandler.Last.TenantId);
        Assert.Equal("pat", CaptureHandler.Last.UserId);
        Assert.Equal("4bf92f3577b34da6a3ce929d0e0e4736", CaptureHandler.Last.TraceId);
        Assert.Null(AmbientMessageContext.Current);
    }

    [Fact]
    public async Task DispatchAsync_MessageContextCorrelation_SurvivesHandleCommandAsync()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(o =>
        {
            o.RequireTenantId = false;
            o.EnableAuthorization = false;
        });
        services.AddBoundedContext("wiring-w6")
            .UseInMemory()
            .WithCommandHandlers([typeof(TickHandler)]);

        var sp = services.BuildServiceProvider();
        var dispatcher = sp.GetRequiredService<ICommandDispatcher>();
        var store = sp.GetRequiredKeyedService<IEventStore>("wiring-w6");

        var command = new TickCommand(Guid.NewGuid(), Guid.NewGuid());
        var inbound = new MessageContext { CorrelationId = "saga-corr-w6" };

        await dispatcher.DispatchAsync(command, inbound);

        var events = await store.ReadStreamAsync($"{nameof(TickAggregate)}:{command.AggregateId}");
        Assert.Equal(2, events.Count);
        Assert.All(events, e => Assert.Equal("saga-corr-w6", e.Metadata.CorrelationId));
        Assert.All(events, e => Assert.Equal(command.Id.ToString(), e.Metadata.CausationId));
        Assert.Equal(events[0].Metadata.CorrelationId, events[1].Metadata.CorrelationId);
        Assert.NotEqual(events[0].Metadata.CausationId, events[0].Metadata.CorrelationId);
        Assert.Null(AmbientMessageContext.Current);
    }

    public sealed record CaptureCommand(Guid Id) : ICommand;

    public sealed class CaptureHandler : ICommandHandler<CaptureCommand>
    {
        public static CommandMetadata? Last;

        private readonly IMetadataProvider _metadata;

        public CaptureHandler(IMetadataProvider metadata) => _metadata = metadata;

        public Task HandleAsync(CaptureCommand command, CancellationToken cancellationToken = default)
        {
            Last = _metadata.CaptureCommandMetadata();
            return Task.CompletedTask;
        }
    }

    public sealed record TickCommand(Guid Id, Guid AggregateId) : ICommand;

    public sealed class TickHandler(IAggregateRepository repository) : ICommandHandler<TickCommand>
    {
        public async Task HandleAsync(TickCommand command, CancellationToken cancellationToken = default)
        {
            var aggregate = await repository.GetOrCreateAsync<TickAggregate>(command.AggregateId, cancellationToken);
            await repository.HandleCommandAsync(aggregate, command, cancellationToken: cancellationToken);
        }
    }

    public sealed class TickState : IState
    {
        public int Count { get; set; }
    }

    public sealed class TickAggregate : AggregateRoot<TickState>
    {
        public void Handle(TickCommand _)
        {
            Apply(new TickApplied(Guid.NewGuid(), DateTime.UtcNow));
            Apply(new TickApplied(Guid.NewGuid(), DateTime.UtcNow));
        }

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is TickApplied)
                State.Count++;
        }
    }

    [EventTypeName("dispatcher-message-context-tests.tick-applied")]
    public sealed record TickApplied(Guid Id, DateTime Timestamp) : IEvent;
}
