using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.TestUtilities;
using Xunit;

namespace LawnDart.EventSourcing.Tests.Aggregates;

public class HandleCommandMetadataTests
{
    private static AggregateRepository CreateRepository(InMemoryEventStore store) =>
        new(store, new DefaultMetadataProvider(), new TestTenantContextProvider(),
            Options.Create(new LawnDartOptions { RequireTenantId = false }));

    [Fact]
    public async Task HandleCommandAsync_SetsCausationIdToCommandId_WhenUnset()
    {
        var store = new InMemoryEventStore();
        var repository = CreateRepository(store);
        var aggregate = await repository.GetOrCreateAsync<TwoTickAggregate>(Guid.NewGuid());
        var command = new TwoTickCommand(Guid.NewGuid());

        await repository.HandleCommandAsync(aggregate, command);

        var events = await store.ReadStreamAsync(aggregate.StreamId);
        Assert.Equal(2, events.Count);
        Assert.All(events, e => Assert.Equal(command.Id.ToString(), e.Metadata.CausationId));
    }

    [Fact]
    public async Task HandleCommandAsync_KeepsCallerCausationId()
    {
        var store = new InMemoryEventStore();
        var repository = CreateRepository(store);
        var aggregate = await repository.GetOrCreateAsync<TwoTickAggregate>(Guid.NewGuid());
        var command = new TwoTickCommand(Guid.NewGuid());

        await repository.HandleCommandAsync(aggregate, command, new CommandMetadata
        {
            CorrelationId = "keep-corr",
            CausationId = "already-set"
        });

        var events = await store.ReadStreamAsync(aggregate.StreamId);
        Assert.All(events, e => Assert.Equal("already-set", e.Metadata.CausationId));
        Assert.All(events, e => Assert.Equal("keep-corr", e.Metadata.CorrelationId));
    }

    [Fact]
    public async Task HandleCommandAsync_CorrelationId_IsStableAcrossEvents()
    {
        var store = new InMemoryEventStore();
        var repository = CreateRepository(store);
        var aggregate = await repository.GetOrCreateAsync<TwoTickAggregate>(Guid.NewGuid());

        await repository.HandleCommandAsync(aggregate, new TwoTickCommand(Guid.NewGuid()));

        var events = await store.ReadStreamAsync(aggregate.StreamId);
        Assert.Equal(2, events.Count);
        Assert.False(string.IsNullOrWhiteSpace(events[0].Metadata.CorrelationId));
        Assert.Equal(events[0].Metadata.CorrelationId, events[1].Metadata.CorrelationId);
        Assert.NotEqual(events[0].Metadata.CausationId, events[0].Metadata.CorrelationId);
    }

    [Fact]
    public async Task HandleCommandAsync_StoresCatalogToken_NotFullName()
    {
        var store = new InMemoryEventStore();
        var repository = CreateRepository(store);
        var aggregate = await repository.GetOrCreateAsync<TwoTickAggregate>(Guid.NewGuid());
        var business = new DateTime(2018, 4, 1, 8, 0, 0, DateTimeKind.Utc);

        await repository.HandleCommandAsync(aggregate, new DatedTickCommand(Guid.NewGuid(), business));

        var stored = Assert.Single(await store.ReadStreamAsync(aggregate.StreamId));
        Assert.Equal("handle-command-metadata-tests.dated-tick-applied", stored.Metadata.SchemaName);
        Assert.NotEqual(typeof(DatedTickApplied).FullName, stored.Metadata.SchemaName);
        Assert.Equal(business, stored.Metadata.Timestamp);
        Assert.NotNull(stored.Metadata.CommitTimestamp);
        Assert.True(stored.Metadata.CommitTimestamp > business);
    }

    private sealed class TickState : IState
    {
        public int Count { get; set; }
    }

    private sealed class TwoTickAggregate : AggregateRoot<TickState>
    {
        public void Handle(TwoTickCommand _)
        {
            Apply(new TickApplied(Guid.NewGuid(), DateTime.UtcNow, 1));
            Apply(new TickApplied(Guid.NewGuid(), DateTime.UtcNow, 2));
        }

        public void Handle(DatedTickCommand command) =>
            Apply(new DatedTickApplied(Guid.NewGuid(), command.BusinessTime));

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is TickApplied or DatedTickApplied)
                State.Count++;
        }
    }

    private sealed record TwoTickCommand(Guid Id) : ICommand;

    private sealed record DatedTickCommand(Guid Id, DateTime BusinessTime) : ICommand;

    [EventTypeName("handle-command-metadata-tests.tick-applied")]
    private sealed record TickApplied(Guid Id, DateTime Timestamp, int N) : IEvent;

    [EventTypeName("handle-command-metadata-tests.dated-tick-applied")]
    private sealed record DatedTickApplied(Guid Id, DateTime Timestamp) : IEvent;
}
