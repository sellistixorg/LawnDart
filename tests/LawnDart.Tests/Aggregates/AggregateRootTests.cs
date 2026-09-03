using LawnDart;
using LawnDart.Aggregates;
using Xunit;

namespace LawnDart.Tests.Aggregates;

public class AggregateRootTests
{
    [Fact]
    public void PendingEvents_InitiallyEmpty()
    {
        // Arrange
        var aggregate = new TestAggregate();

        // Assert
        Assert.Empty(aggregate.PendingEvents);
        Assert.Equal(0, aggregate.Version);
    }

    [Fact]
    public void Apply_AddsEventToPending()
    {
        // Arrange
        var aggregate = new TestAggregate();
        var @event = new TestEvent(Guid.NewGuid(), DateTime.UtcNow);

        // Act
        aggregate.ApplyTest(@event);

        // Assert
        Assert.Single(aggregate.PendingEvents);
        Assert.Equal(1, aggregate.Version);
    }

    [Fact]
    public void ReplayEvents_RestoresState()
    {
        // Arrange
        var aggregate = new TestAggregate();
        var events = new[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        };

        // Act
        aggregate.ReplayEvents(events);

        // Assert
        Assert.Equal(2, aggregate.Version);
        Assert.Equal(2, aggregate.AppliedEventCount);
    }

    [Fact]
    public void SetStreamId_SetsStreamId()
    {
        // Arrange
        var aggregate = new TestAggregate();

        // Act
        aggregate.SetStreamId("test-stream");

        // Assert
        Assert.Equal("test-stream", aggregate.StreamId);
    }

    [Fact]
    public void ClearPendingEvents_RemovesAllEvents()
    {
        // Arrange
        var aggregate = new TestAggregate();
        aggregate.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));
        Assert.Single(aggregate.PendingEvents);

        // Act
        aggregate.ClearPendingEvents();

        // Assert
        Assert.Empty(aggregate.PendingEvents);
    }

    private class TestState : IState
    {
        public int EventCount { get; set; }
    }

    private class TestAggregate : AggregateRoot<TestState>
    {
        public int AppliedEventCount { get; private set; }

        public void ApplyTest(IEvent @event)
        {
            Apply(@event);
        }

        public override Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        protected override void ApplyEventToState(IEvent @event)
        {
            AppliedEventCount++;
            State.EventCount = AppliedEventCount;
        }
    }

    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}


