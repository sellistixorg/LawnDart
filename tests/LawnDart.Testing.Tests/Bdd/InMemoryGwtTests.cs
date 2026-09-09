using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.Testing.Bdd;

namespace LawnDart.Testing.Tests.Bdd;

public sealed class InMemoryGwtTests
{
    [Fact]
    public async Task aggregate_spec_given_when_then_on_in_memory()
    {
        await using var ctx = BddTestContext.CreateInMemory();
        var id = Guid.NewGuid();

        var result = await AggregateSpec
            .For<Counter>(ctx, id)
            .Given(new CounterCreated(Guid.NewGuid(), DateTime.UtcNow, id))
            .When(new IncrementCommand(Guid.NewGuid(), id))
            .ThenEmittedEvent<CounterIncremented>(e => e.CounterId == id)
            .AndExpectedVersion(2)
            .RunAsync();

        Assert.Single(result.EmittedEvents);
        Assert.Equal(1, result.Aggregate.State.Value);
    }

    [Fact]
    public async Task dcb_spec_given_when_then_on_in_memory()
    {
        await using var ctx = BddTestContext.CreateInMemory();
        var id = Guid.NewGuid();
        var tags = new[] { $"counter:{id:N}" };

        var result = await DcbSpec
            .For<CounterEntity, IncrementCommand>(ctx, tags, new IncrementCommand(Guid.NewGuid(), id))
            .Given(new CounterCreated(Guid.NewGuid(), DateTime.UtcNow, id), tags)
            .ThenEmittedEvent<CounterIncremented>(e => e.CounterId == id)
            .RunAsync();

        Assert.Single(result.EmittedEvents);
    }

    public sealed class CounterState : IState
    {
        public int Value { get; set; }
    }

    public sealed class Counter : AggregateRoot<CounterState>
    {
        public void Handle(IncrementCommand increment) =>
            Apply(new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, increment.CounterId));

        protected override void ApplyEventToState(IEvent @event)
        {
            switch (@event)
            {
                case CounterCreated:
                    State.Value = 0;
                    break;
                case CounterIncremented:
                    State.Value++;
                    break;
            }
        }
    }

    public sealed class CounterEntity : DcbEntity
    {
        public int Value { get; private set; }

        public void Handle(IncrementCommand increment) =>
            Emit(new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, increment.CounterId));

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is CounterCreated)
                Value = 0;
            if (@event is CounterIncremented)
                Value++;
        }
    }

    public sealed record IncrementCommand(Guid Id, Guid CounterId) : ICommand;

    public sealed record CounterCreated(Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;

    public sealed record CounterIncremented(Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;
}
