using Xunit;
using LawnDart.Dcb;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.EventSourcing.EventStore;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.EventSourcing.Tests.Dcb;

public class DcbProjectorTests
{
    [Fact]
    public void GetTagsForProjection_ReturnsConfiguredTags()
    {
        // Arrange
        var projector = new TestProjector();

        // Act
        var tags = projector.GetTagsForProjection();

        // Assert
        Assert.Equal(new[] { "order", "payment" }, tags);
    }

    [Fact]
    public async Task ProjectEventAsync_ProcessesEvent()
    {
        // Arrange
        var projector = new TestProjector();
        var @event = new TestProjectionEvent { Value = "test" };
        var metadata = new EventMetadata();

        // Act
        await projector.ProjectEventAsync(@event, metadata);

        // Assert
        Assert.Single(projector.ProcessedEvents);
        Assert.Equal("test", projector.ProcessedEvents[0]);
    }

    [Fact]
    public async Task InitializeFromHistoryAsync_ReplaysAllEvents()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: true, logger: NullLogger<InMemoryEventStore>.Instance);
        var projector = new TestProjector();

        // Add events with matching tags
        await eventStore.AppendAsync(
            "stream1",
            new IEvent[]
            {
                new TestProjectionEvent { Value = "event1" },
                new TestProjectionEvent { Value = "event2" }
            },
            null,
            new EventMetadata(),
            new[] { "order" });

        // Act
        await projector.InitializeFromHistoryAsync(eventStore);

        // Assert
        Assert.Equal(2, projector.ProcessedEvents.Count);
        Assert.Contains("event1", projector.ProcessedEvents);
        Assert.Contains("event2", projector.ProcessedEvents);
    }

    [Fact]
    public async Task InitializeFromHistoryAsync_OrdersBySequencePosition()
    {
        // Arrange
        var eventStore = new InMemoryEventStore(enableRegistry: true, logger: NullLogger<InMemoryEventStore>.Instance);
        var projector = new TestProjector();

        // Add events in specific order
        await eventStore.AppendAsync(
            "stream1",
            new IEvent[] { new TestProjectionEvent { Value = "first" } },
            null,
            new EventMetadata(),
            new[] { "order" });

        await eventStore.AppendAsync(
            "stream2",
            new IEvent[] { new TestProjectionEvent { Value = "second" } },
            null,
            new EventMetadata(),
            new[] { "order" });

        // Act
        await projector.InitializeFromHistoryAsync(eventStore);

        // Assert
        Assert.Equal(new[] { "first", "second" }, projector.ProcessedEvents);
    }
}

public class DcbReactorTests
{
    [Fact]
    public void GetTagsForReaction_ReturnsConfiguredTags()
    {
        // Arrange
        var reactor = new TestReactor();

        // Act
        var tags = reactor.GetTagsForReaction();

        // Assert
        Assert.Equal(new[] { "order-created" }, tags);
    }

    [Fact]
    public async Task ReactAsync_WithMatchingEvent_EmitsCommands()
    {
        // Arrange
        var reactor = new TestReactor();
        var @event = new TestReactionEvent { Value = "test" };
        var metadata = new EventMetadata();

        // Act
        var commands = await reactor.ReactAsync(@event, metadata);

        // Assert
        Assert.Single(commands);
        var command = commands.First() as TestReactionCommand;
        Assert.NotNull(command);
        Assert.Equal("test", command.Value);
    }

    [Fact]
    public async Task ReactAsync_WithNonMatchingEvent_ReturnsEmpty()
    {
        // Arrange
        var reactor = new TestReactor();
        var @event = new TestProjectionEvent { Value = "test" }; // Wrong event type
        var metadata = new EventMetadata();

        // Act
        var commands = await reactor.ReactAsync(@event, metadata);

        // Assert
        Assert.Empty(commands);
    }

    [Fact]
    public async Task ReactAsync_CanEmitMultipleCommands()
    {
        // Arrange
        var reactor = new MultiCommandReactor();
        var @event = new TestReactionEvent { Value = "test" };
        var metadata = new EventMetadata();

        // Act
        var commands = await reactor.ReactAsync(@event, metadata);

        // Assert
        Assert.Equal(2, commands.Count());
    }
}

// Test types
[EventTypeName("dcb-patterns-tests.test-projection-event")]
public class TestProjectionEvent : IEvent
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public string Value { get; init; } = string.Empty;
}
[EventTypeName("dcb-patterns-tests.test-reaction-event")]

public class TestReactionEvent : IEvent
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public string Value { get; init; } = string.Empty;
}

public class TestReactionCommand : ICommand
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public string Value { get; init; } = string.Empty;
}

public class TestProjector : DcbProjector<TestState>
{
    public List<string> ProcessedEvents { get; } = new();

    public override string[] GetTagsForProjection()
    {
        return new[] { "order", "payment" };
    }

    public override Task ProjectEventAsync(IEvent @event, EventMetadata metadata, CancellationToken cancellationToken = default)
    {
        if (@event is TestProjectionEvent testEvent)
        {
            ProcessedEvents.Add(testEvent.Value);
        }
        return Task.CompletedTask;
    }
}

public class TestReactor : DcbReactor<TestReactionEvent>
{
    public override string[] GetTagsForReaction()
    {
        return new[] { "order-created" };
    }

    protected override Task<IEnumerable<ICommand>> ReactToEventAsync(
        TestReactionEvent @event, 
        EventMetadata metadata, 
        CancellationToken cancellationToken = default)
    {
        var commands = new ICommand[]
        {
            new TestReactionCommand { Value = @event.Value }
        };
        return Task.FromResult<IEnumerable<ICommand>>(commands);
    }
}

public class MultiCommandReactor : DcbReactor<TestReactionEvent>
{
    public override string[] GetTagsForReaction()
    {
        return new[] { "order-created" };
    }

    protected override Task<IEnumerable<ICommand>> ReactToEventAsync(
        TestReactionEvent @event, 
        EventMetadata metadata, 
        CancellationToken cancellationToken = default)
    {
        var commands = new ICommand[]
        {
            new TestReactionCommand { Value = @event.Value + "-1" },
            new TestReactionCommand { Value = @event.Value + "-2" }
        };
        return Task.FromResult<IEnumerable<ICommand>>(commands);
    }
}
