using Xunit;
using LawnDart.Dcb;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Tests.Dcb;

public class DcbEntityTests
{
    [Fact]
    public void NewEntity_HasZeroSequencePosition()
    {
        // Arrange & Act
        var entity = new TestDcbEntityForEntityTests();

        // Assert
        Assert.Equal(0, entity.LastSequencePosition);
        Assert.Empty(entity.Tags);
        Assert.Empty(entity.PendingEvents);
    }

    [Fact]
    public void SetTags_UpdatesTags()
    {
        // Arrange
        var entity = new TestDcbEntityForEntityTests();

        // Act
        entity.SetTags("tag1", "tag2");

        // Assert
        Assert.Equal(new[] { "tag1", "tag2" }, entity.Tags);
    }

    [Fact]
    public void SetLastSequencePosition_UpdatesPosition()
    {
        // Arrange
        var entity = new TestDcbEntityForEntityTests();

        // Act
        entity.SetLastSequencePosition(42);

        // Assert
        Assert.Equal(42, entity.LastSequencePosition);
    }

    [Fact]
    public async Task Emit_AddsToPendingEvents()
    {
        // Arrange
        var entity = new TestDcbEntityForEntityTests();
        entity.SetTags("order:123");

        // Act
        await entity.HandleAsync(new TestCommand { Value = "test" });

        // Assert
        Assert.Single(entity.PendingEvents);
        var evt = entity.PendingEvents.First() as TestEvent;
        Assert.NotNull(evt);
        Assert.Equal("test", evt.Value);
    }

    [Fact]
    public async Task Emit_AppliesEventToState()
    {
        // Arrange
        var entity = new TestDcbEntityForEntityTests();
        entity.SetTags("order:123");

        // Act
        await entity.HandleAsync(new TestCommand { Value = "test" });

        // Assert
        Assert.Equal(1, entity.State.EventCount);
        Assert.Contains("test", entity.State.Values);
    }

    [Fact]
    public async Task Emit_WithTags_MergesTags()
    {
        // Arrange
        var entity = new TestDcbEntityForEntityTests();
        entity.SetTags("tag1");

        // Act
        await entity.HandleAsync(new TestCommand { Value = "test" });

        // Assert
        Assert.Contains("tag1", entity.Tags);
        Assert.Contains("order:123", entity.Tags); // Added by Emit
    }

    [Fact]
    public async Task ClearPendingEvents_RemovesAllPendingEvents()
    {
        // Arrange
        var entity = new TestDcbEntityForEntityTests();
        entity.SetTags("order:123");
        await entity.HandleAsync(new TestCommand { Value = "test" });

        // Act
        entity.ClearPendingEvents();

        // Assert
        Assert.Empty(entity.PendingEvents);
        Assert.Equal(1, entity.State.EventCount); // State should remain
    }

    [Fact]
    public void ReplayEvents_RebuildsSate()
    {
        // Arrange
        var entity = new TestDcbEntityForEntityTests();
        var events = new[]
        {
            new SequencedEvent(
                @event: new TestEvent { Value = "event1" },
                sequencePosition: 1,
                streamId: "stream1",
                version: 0,
                metadata: new Metadata.EventMetadata(),
                tags: new[] { "tag1" }),
            new SequencedEvent(
                @event: new TestEvent { Value = "event2" },
                sequencePosition: 2,
                streamId: "stream1",
                version: 0,
                metadata: new Metadata.EventMetadata(),
                tags: new[] { "tag2" })
        };

        // Act
        entity.ReplayEvents(events);

        // Assert
        Assert.Equal(2, entity.State.EventCount);
        Assert.Contains("event1", entity.State.Values);
        Assert.Contains("event2", entity.State.Values);
        Assert.Equal(2, entity.LastSequencePosition);
        Assert.Contains("tag1", entity.Tags);
        Assert.Contains("tag2", entity.Tags);
    }

    [Fact]
    public void ReplayEvents_OrdersBySequencePosition()
    {
        // Arrange
        var entity = new TestDcbEntityForEntityTests();
        var events = new[]
        {
            new SequencedEvent(
                @event: new TestEvent { Value = "second" },
                sequencePosition: 2,
                streamId: "stream1",
                version: 0,
                metadata: new Metadata.EventMetadata(),
                tags: Array.Empty<string>()),
            new SequencedEvent(
                @event: new TestEvent { Value = "first" },
                sequencePosition: 1,
                streamId: "stream1",
                version: 0,
                metadata: new Metadata.EventMetadata(),
                tags: Array.Empty<string>())
        };

        // Act
        entity.ReplayEvents(events);

        // Assert
        Assert.Equal(new[] { "first", "second" }, entity.State.Values);
    }

    [Fact]
    public void ReplayEvents_DoesNotMutateConsistencyTags()
    {
        var entity = new TestDcbEntityForEntityTests();
        entity.SetConsistencyTags("product:sku-1");

        entity.ReplayEvents(new[]
        {
            new SequencedEvent(
                @event: new TestEvent { Value = "event1" },
                sequencePosition: 1,
                streamId: "stream1",
                version: 0,
                metadata: new Metadata.EventMetadata(),
                tags: new[] { "product:sku-1", "order:order-9" })
        });

        Assert.Equal(new[] { "product:sku-1" }, entity.ConsistencyTags);
        Assert.Contains("product:sku-1", entity.Tags);
        Assert.Contains("order:order-9", entity.Tags);
    }

    [Fact]
    public async Task Emit_DoesNotMutateConsistencyTags()
    {
        var entity = new TestDcbEntityForEntityTests();
        entity.SetTags("tag1");
        entity.SetConsistencyTags("tag1");

        await entity.HandleAsync(new TestCommand { Value = "test" });

        Assert.Equal(new[] { "tag1" }, entity.ConsistencyTags);
        Assert.Contains("order:123", entity.Tags);
    }

    [Fact]
    public void SetTags_DoesNotChangeConsistencyTags()
    {
        var entity = new TestDcbEntityForEntityTests();
        entity.SetConsistencyTags("fence:a");
        entity.SetTags("payload:b");

        Assert.Equal(new[] { "fence:a" }, entity.ConsistencyTags);
        Assert.Equal(new[] { "payload:b" }, entity.Tags);
    }

    [Fact]
    public void ReplayEvents_IncrementsEventsSinceLastSnapshot()
    {
        var entity = new TestDcbEntityForEntityTests();
        entity.ReplayEvents(new[]
        {
            new SequencedEvent(
                new TestEvent { Value = "a" }, 1, "s", 0, new Metadata.EventMetadata(), Array.Empty<string>()),
            new SequencedEvent(
                new TestEvent { Value = "b" }, 2, "s", 0, new Metadata.EventMetadata(), Array.Empty<string>())
        });

        Assert.Equal(2, entity.EventsSinceLastSnapshot);
    }

    [Fact]
    public void SetSnapshotCursor_ResetsEventsSinceLastSnapshot_ThenReplayCountsDelta()
    {
        var entity = new TestDcbEntityForEntityTests();
        entity.ReplayEvents(new[]
        {
            new SequencedEvent(
                new TestEvent { Value = "old" }, 10, "s", 0, new Metadata.EventMetadata(), Array.Empty<string>())
        });
        Assert.Equal(1, entity.EventsSinceLastSnapshot);

        var taken = DateTime.UtcNow;
        entity.SetSnapshotCursor(10, taken);

        Assert.Equal(0, entity.EventsSinceLastSnapshot);
        Assert.Equal(10, entity.LastSequencePosition);
        Assert.Equal(taken, entity.LastSnapshotUtc);

        entity.ReplayEvents(new[]
        {
            new SequencedEvent(
                new TestEvent { Value = "delta" }, 11, "s", 0, new Metadata.EventMetadata(), Array.Empty<string>())
        });

        Assert.Equal(1, entity.EventsSinceLastSnapshot);
        Assert.Equal(11, entity.LastSequencePosition);
    }

    [Fact]
    public void RecordFlushedEvents_ThenNoteSnapshotWritten_ResetsCount()
    {
        var entity = new TestDcbEntityForEntityTests();
        entity.RecordFlushedEvents(3);
        Assert.Equal(3, entity.EventsSinceLastSnapshot);

        var taken = DateTime.UtcNow;
        entity.NoteSnapshotWritten(42, taken);

        Assert.Equal(0, entity.EventsSinceLastSnapshot);
        Assert.Equal(42, entity.LastSnapshotGlobalSequence);
        Assert.Equal(taken, entity.LastSnapshotUtc);
    }
}

public class TestDcbEntityForEntityTests : DcbEntity<TestState>
{
    public override Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
    {
        if (command is TestCommand testCommand)
        {
            // Emit with a specific tag
            Emit(new TestEvent { Value = testCommand.Value }, "order:123");
        }
        return Task.CompletedTask;
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        State.ApplyEventToState(@event);
    }
}
