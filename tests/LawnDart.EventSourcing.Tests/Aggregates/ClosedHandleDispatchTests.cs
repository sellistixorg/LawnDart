using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventSourcing.Performance;
using LawnDart.Metadata;
using LawnDart.TestUtilities;
using Xunit;

using LawnDart.EventStore;
namespace LawnDart.EventSourcing.Tests.Aggregates;

public class ClosedHandleDispatchTests
{
    private static IOptions<LawnDartOptions> CreateOptions() =>
        Options.Create(new LawnDartOptions { RequireTenantId = false });

    private static AggregateRepository CreateRepository() =>
        new(
            new InMemoryEventStore(),
            new DefaultMetadataProvider(),
            new TestTenantContextProvider(),
            CreateOptions());

    [Fact]
    public async Task HandleCommandAsync_ClosedHandle_AppliesEvent()
    {
        var repository = CreateRepository();
        var aggregate = await repository.GetOrCreateAsync<ClosedHandleAggregate>(Guid.NewGuid());

        await repository.HandleCommandAsync(aggregate, new ClosedIncrementCommand(Guid.NewGuid(), 4));

        Assert.Equal(4, aggregate.State.Total);
        Assert.Empty(aggregate.PendingEvents);
    }

    [Fact]
    public async Task HandleCommandAsync_ClosedHandle_UnknownCommand_Throws()
    {
        var repository = CreateRepository();
        var aggregate = await repository.GetOrCreateAsync<ClosedHandleAggregate>(Guid.NewGuid());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            repository.HandleCommandAsync(aggregate, new UnknownCommand(Guid.NewGuid())));

        Assert.Contains("UnknownCommand", ex.Message);
    }

    [Fact]
    public async Task HandleCommandAsync_GenericOverride_StillWins()
    {
        var repository = CreateRepository();
        var aggregate = await repository.GetOrCreateAsync<GenericOverrideAggregate>(Guid.NewGuid());

        await repository.HandleCommandAsync(aggregate, new ClosedIncrementCommand(Guid.NewGuid(), 4));

        Assert.True(aggregate.GenericCalled);
        Assert.Equal(100, aggregate.State.Total);
    }

    [Fact]
    public async Task HandleCommandAsync_GenericOverride_UnknownCommand_DoesNotThrow()
    {
        var repository = CreateRepository();
        var aggregate = await repository.GetOrCreateAsync<GenericOverrideAggregate>(Guid.NewGuid());

        await repository.HandleCommandAsync(aggregate, new UnknownCommand(Guid.NewGuid()));

        Assert.True(aggregate.GenericCalled);
        Assert.Equal(0, aggregate.State.Total);
    }

    [Fact]
    public async Task HandleCommandAsync_PrefersHandleWithCancellationToken()
    {
        var repository = CreateRepository();
        var aggregate = await repository.GetOrCreateAsync<CancellationHandleAggregate>(Guid.NewGuid());

        await repository.HandleCommandAsync(aggregate, new ClosedIncrementCommand(Guid.NewGuid(), 1));

        Assert.True(aggregate.UsedCancellationOverload);
        Assert.Equal(1, aggregate.State.Total);
    }

    [Fact]
    public void OverridesHandleAsync_DetectsGenericAndClosedTypes()
    {
        Assert.False(CompiledCommandApplicator.OverridesHandleAsync(typeof(ClosedHandleAggregate)));
        Assert.True(CompiledCommandApplicator.OverridesHandleAsync(typeof(GenericOverrideAggregate)));
    }

    private sealed class CounterState : IState
    {
        public int Total { get; set; }
    }

    private sealed class ClosedHandleAggregate : AggregateRoot<CounterState>
    {
        public void Handle(ClosedIncrementCommand command) =>
            Apply(new ClosedIncremented(Guid.NewGuid(), DateTime.UtcNow, command.Amount));

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is ClosedIncremented incremented)
                State.Total += incremented.Amount;
        }
    }

    private sealed class GenericOverrideAggregate : AggregateRoot<CounterState>
    {
        public bool GenericCalled { get; private set; }

        [Obsolete("Declare Handle(TCommand) methods instead.")]
        public override Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
        {
            GenericCalled = true;
            if (command is ClosedIncrementCommand)
                Apply(new ClosedIncremented(Guid.NewGuid(), DateTime.UtcNow, 100));
            return Task.CompletedTask;
        }

        public void Handle(ClosedIncrementCommand command) =>
            Apply(new ClosedIncremented(Guid.NewGuid(), DateTime.UtcNow, command.Amount));

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is ClosedIncremented incremented)
                State.Total += incremented.Amount;
        }
    }

    private sealed class CancellationHandleAggregate : AggregateRoot<CounterState>
    {
        public bool UsedCancellationOverload { get; private set; }

        public void Handle(ClosedIncrementCommand command) =>
            Apply(new ClosedIncremented(Guid.NewGuid(), DateTime.UtcNow, command.Amount));

        public void Handle(ClosedIncrementCommand command, CancellationToken cancellationToken)
        {
            UsedCancellationOverload = true;
            Apply(new ClosedIncremented(Guid.NewGuid(), DateTime.UtcNow, command.Amount));
        }

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is ClosedIncremented incremented)
                State.Total += incremented.Amount;
        }
    }

    private sealed record ClosedIncrementCommand(Guid Id, int Amount) : ICommand;
    private sealed record UnknownCommand(Guid Id) : ICommand;
    [EventTypeName("closed-handle-dispatch-tests.closed-incremented")]
    private sealed record ClosedIncremented(Guid Id, DateTime Timestamp, int Amount) : IEvent;
}
