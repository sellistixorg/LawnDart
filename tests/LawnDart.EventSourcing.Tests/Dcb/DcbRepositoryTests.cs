using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventStore;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.EventStore;
using LawnDart.Metadata;
using LawnDart.Snapshots;
using LawnDart.Tagging;
using LawnDart.TestUtilities;
using Xunit;

namespace LawnDart.EventSourcing.Tests.Dcb;

public class DcbRepositoryTests
{
    private readonly InMemoryEventStore _eventStore;
    private readonly TestMetadataProvider _metadataProvider;
    private readonly TestTenantContextProvider _tenantProvider;
    private readonly DcbRepository _repository;

    public DcbRepositoryTests()
    {
        _eventStore = new InMemoryEventStore(enableRegistry: true, logger: NullLogger<InMemoryEventStore>.Instance);
        _metadataProvider = new TestMetadataProvider();
        _tenantProvider = new TestTenantContextProvider("tenant1");

        var options = Options.Create(new LawnDartOptions());

        _repository = new DcbRepository(
            _eventStore,
            _metadataProvider,
            _tenantProvider,
            options,
            null,
            null,
            NullLogger<DcbRepository>.Instance);
    }

    [Fact]
    public async Task GetStateAsync_WithNoEvents_ReturnsEmptyState()
    {
        // Arrange
        var tags = new[] { "order:123" };

        // Act
        var state = await _repository.GetStateAsync<TestState>(tags);

        // Assert
        Assert.NotNull(state);
        Assert.Equal(0, state.EventCount);
    }

    [Fact]
    public async Task GetStateAsync_WithEvents_RebuildsSate()
    {
        // Arrange
        var tags = new[] { "order:123" };
        var events = new IEvent[]
        {
            new TestEvent { Value = "event1" },
            new TestEvent { Value = "event2" }
        };

        await _eventStore.AppendAsync(
            "tenant1:stream:stream1",
            events,
            null,
            new EventMetadata(),
            tags);

        // Act
        var state = await _repository.GetStateAsync<TestState>(tags);

        // Assert
        Assert.Equal(2, state.EventCount);
        Assert.Contains("event1", state.Values);
        Assert.Contains("event2", state.Values);
    }

    [Fact]
    public async Task GetLastSequencePositionAsync_WithNoEvents_ReturnsZero()
    {
        // Arrange
        var tags = new[] { "order:123" };

        // Act
        var position = await _repository.GetLastSequencePositionAsync(tags);

        // Assert
        Assert.Equal(0L, position);
    }

    [Fact]
    public async Task GetLastSequencePositionAsync_WithEvents_ReturnsMaxPosition()
    {
        // Arrange
        var tags = new[] { "order:123" };
        var events = new IEvent[]
        {
            new TestEvent { Value = "event1" },
            new TestEvent { Value = "event2" }
        };

        await _eventStore.AppendAsync(
            "tenant1:stream:stream1",
            events,
            null,
            new EventMetadata(),
            tags);

        // Act
        var position = await _repository.GetLastSequencePositionAsync(tags);

        // Assert
        Assert.True(position > 0);
    }

    [Fact]
    public async Task AppendWithContextAsync_AppendsEvents()
    {
        // Arrange
        var tags = new[] { "order:123" };
        var events = new IEvent[]
        {
            new TestEvent { Value = "event1" }
        };

        // Act
        var positions = await _repository.AppendWithContextAsync(events, tags);

        // Assert
        Assert.Single(positions);
        Assert.True(positions[0] > 0);
    }

    [Fact]
    public async Task AppendWithContextAsync_WithCondition_EnforcesCondition()
    {
        // Arrange
        var tags = new[] { "order:123" };
        var events = new IEvent[]
        {
            new TestEvent { Value = "event1" }
        };

        // First append
        await _repository.AppendWithContextAsync(events, tags);

        // Act - Second append with condition expecting no new events
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByTags(tags)),
            0);

        // Assert
        await Assert.ThrowsAsync<ConcurrencyException>(async () =>
            await _repository.AppendWithContextAsync(events, tags, condition));
    }

    [Fact]
    public async Task CreateEntityAsync_SetsConsistencyTagsFromLoadQuery()
    {
        var tags = new[] { "order:123" };

        var entity = await _repository.CreateEntityAsync<TestDcbEntity>(tags);

        Assert.Equal(tags, entity.ConsistencyTags);
        Assert.Equal(tags, entity.Tags);
    }

    [Fact]
    public async Task GetOrCreateEntityAsync_KeepsLoadQueryAsConsistencyFence_WhenEventsCarryExtraTags()
    {
        var loadTags = new[] { "product:sku-1" };
        await _eventStore.AppendAsync(
            "tenant1:stream:inventory",
            new IEvent[] { new TestEvent { Value = "reserved" } },
            null,
            new EventMetadata(),
            new[] { "product:sku-1", "order:order-9" });

        var entity = await _repository.GetOrCreateEntityAsync<TestDcbEntity>(loadTags);

        Assert.Equal(loadTags, entity.ConsistencyTags);
        Assert.Contains("product:sku-1", entity.Tags);
        Assert.Contains("order:order-9", entity.Tags);
        Assert.Equal(1, entity.State.EventCount);
    }

    [Fact]
    public async Task HandleCommandAsync_FencesOnLoadQuery_NotAccumulatedTagAnd()
    {
        // Load with a single product tag. Stored events also carry an order tag, so the
        // accumulated payload-tag union is AND-narrower than the load query. A writer
        // that only stamps the product tag must still trip the fence.
        var loadTags = new[] { "product:sku-1" };
        await _repository.AppendWithContextAsync(
            new IEvent[] { new TestEvent { Value = "seed" } },
            new[] { "product:sku-1", "order:order-9" });

        var entity = await _repository.GetOrCreateEntityAsync<TestDcbEntity>(loadTags);
        Assert.Equal(loadTags, entity.ConsistencyTags);

        await _repository.AppendWithContextAsync(
            new IEvent[] { new TestEvent { Value = "racer" } },
            loadTags);

        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            _repository.HandleCommandAsync(
                entity,
                new TestCommand { Value = "decision" },
                new CommandMetadata { UserId = "test-user" }));
    }

    [Fact]
    public async Task HandleCommandAsync_WithSingleLoadTag_StillPersists()
    {
        var tags = new[] { "order:123" };
        var entity = await _repository.CreateEntityAsync<TestDcbEntity>(tags);

        var result = await _repository.HandleCommandAsync(
            entity,
            new TestCommand { Value = "ok" },
            new CommandMetadata { UserId = "test-user" });

        Assert.Equal(1, result.State.EventCount);
        Assert.Empty(result.PendingEvents);
        Assert.Equal(tags, result.ConsistencyTags);
    }

    [Fact]
    public async Task CreateEntityAsync_CreatesNewEntity()
    {
        // Arrange
        var tags = new[] { "order:123" };

        // Act
        var entity = await _repository.CreateEntityAsync<TestDcbEntity>(tags);

        // Assert
        Assert.NotNull(entity);
        Assert.Equal(tags, entity.Tags);
        Assert.Equal(0, entity.LastSequencePosition);
    }

    [Fact]
    public async Task GetOrCreateEntityAsync_WithNoEvents_CreatesNewEntity()
    {
        // Arrange
        var tags = new[] { "order:123" };

        // Act
        var entity = await _repository.GetOrCreateEntityAsync<TestDcbEntity>(tags);

        // Assert
        Assert.NotNull(entity);
        Assert.Equal(tags, entity.Tags);
        Assert.Equal(0, entity.State.EventCount);
    }

    [Fact]
    public async Task GetOrCreateEntityAsync_WithExistingEvents_LoadsEntity()
    {
        // Arrange
        var tags = new[] { "order:123" };
        var events = new IEvent[]
        {
            new TestEvent { Value = "event1" },
            new TestEvent { Value = "event2" }
        };

        await _eventStore.AppendAsync(
            "tenant1:stream:stream1",
            events,
            null,
            new EventMetadata(),
            tags);

        // Act
        var entity = await _repository.GetOrCreateEntityAsync<TestDcbEntity>(tags);

        // Assert
        Assert.NotNull(entity);
        Assert.Equal(2, entity.State.EventCount);
        Assert.True(entity.LastSequencePosition > 0);
    }

    [Fact]
    public async Task HandleCommandAsync_ExecutesCommandAndPersistsEvents()
    {
        // Arrange
        var tags = new[] { "order:123" };
        var entity = await _repository.CreateEntityAsync<TestDcbEntity>(tags);
        var command = new TestCommand { Value = "test" };
        var metadata = new CommandMetadata { UserId = "test-user" };

        // Act
        var result = await _repository.HandleCommandAsync(entity, command, metadata);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(1, result.State.EventCount);
        Assert.Empty(result.PendingEvents);
        Assert.True(result.LastSequencePosition > 0);
    }

    [Fact]
    public async Task HandleCommandAsync_WithMultipleCommands_MaintainsSequence()
    {
        // Arrange
        var tags = new[] { "order:123" };
        var entity = await _repository.CreateEntityAsync<TestDcbEntity>(tags);
        var metadata = new CommandMetadata { UserId = "test-user" };

        // Act
        entity = await _repository.HandleCommandAsync(
            entity, 
            new TestCommand { Value = "first" }, 
            metadata);
        
        entity = await _repository.HandleCommandAsync(
            entity, 
            new TestCommand { Value = "second" }, 
            metadata);

        // Assert
        Assert.Equal(2, entity.State.EventCount);
        Assert.Contains("first", entity.State.Values);
        Assert.Contains("second", entity.State.Values);
    }

    [Fact]
    public async Task AppendWithContextAsync_WithTagProvider_AddsAdditionalTags()
    {
        // Arrange
        var tagProvider = new TestTagProvider(new[] { "auto-tag" });
        var options = Options.Create(new LawnDartOptions());
        var repo = new DcbRepository(
            _eventStore,
            _metadataProvider,
            _tenantProvider,
            options,
            tagProvider,
            null,
            NullLogger<DcbRepository>.Instance);

        var tags = new[] { "order:123" };
        var events = new IEvent[] { new TestEvent { Value = "test" } };

        // Act
        await repo.AppendWithContextAsync(events, tags);

        // Verify the event can be queried by both original and auto tags
        var state1 = await repo.GetStateAsync<TestState>(new[] { "order:123" });
        var state2 = await repo.GetStateAsync<TestState>(new[] { "auto-tag" });

        // Assert
        Assert.Equal(1, state1.EventCount);
        Assert.Equal(1, state2.EventCount);
    }

    [Fact]
    public async Task GetStateAsync_WithMultipleStreams_AggregatesAll()
    {
        // Arrange
        var tags = new[] { "order:123" };
        
        await _eventStore.AppendAsync(
            "stream1",
            new IEvent[] { new TestEvent { Value = "stream1-event" } },
            null,
            new EventMetadata(),
            tags);

        await _eventStore.AppendAsync(
            "stream2",
            new IEvent[] { new TestEvent { Value = "stream2-event" } },
            null,
            new EventMetadata(),
            tags);

        // Act
        var state = await _repository.GetStateAsync<TestState>(tags);

        // Assert
        Assert.Equal(2, state.EventCount);
        Assert.Contains("stream1-event", state.Values);
        Assert.Contains("stream2-event", state.Values);
    }

    [Fact]
    public async Task GetStateAsync_WithMultipleTags_ReturnsMatchingEvents()
    {
        // Arrange
        await _eventStore.AppendAsync(
            "stream1",
            new IEvent[] { new TestEvent { Value = "both-tags" } },
            null,
            new EventMetadata(),
            new[] { "tag1", "tag2" });

        await _eventStore.AppendAsync(
            "stream2",
            new IEvent[] { new TestEvent { Value = "only-tag1" } },
            null,
            new EventMetadata(),
            new[] { "tag1" });

        // Act - Query with both tags
        var state = await _repository.GetStateAsync<TestState>(new[] { "tag1", "tag2" });

        // Assert - Should only get events that have ALL tags
        Assert.Equal(1, state.EventCount);
        Assert.Contains("both-tags", state.Values);
    }

    [Fact]
    public async Task HandleCommandAsync_EventCountStrategy1_WritesSnapshot()
    {
        var snapshotStore = new RecordingDcbSnapshotStore();
        var resolver = new SnapshotStrategyResolver();
        resolver.RegisterForDcb<TestDcbEntity>(new EventCountSnapshotStrategy(1));
        var repo = CreateRepository(snapshotStore, resolver);

        var entity = await repo.CreateEntityAsync<TestDcbEntity>(new[] { "order:snap-1" });
        await repo.HandleCommandAsync(entity, new TestCommand { Value = "one" }, new CommandMetadata { UserId = "u" });

        await snapshotStore.WaitForSaveAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(1, snapshotStore.SaveCount);
        Assert.Equal(0, entity.EventsSinceLastSnapshot);
        Assert.NotNull(entity.LastSnapshotUtc);
        Assert.Equal(DcbSnapshotId.HexLength, snapshotStore.LastDcbId!.Length);
        Assert.Equal(DcbSnapshotId.FromLoadTags(["order:snap-1"]), snapshotStore.LastDcbId);
        Assert.Equal(["order:snap-1"], snapshotStore.LastLoadTags);
    }

    [Fact]
    public async Task HandleCommandAsync_EventCountStrategy5_DoesNotWriteOnSingleEvent()
    {
        var snapshotStore = new RecordingDcbSnapshotStore();
        var resolver = new SnapshotStrategyResolver();
        resolver.RegisterForDcb<TestDcbEntity>(new EventCountSnapshotStrategy(5));
        var repo = CreateRepository(snapshotStore, resolver);

        var entity = await repo.CreateEntityAsync<TestDcbEntity>(new[] { "order:snap-5" });
        await repo.HandleCommandAsync(entity, new TestCommand { Value = "one" }, new CommandMetadata { UserId = "u" });

        Assert.Equal(1, entity.EventsSinceLastSnapshot);
        Assert.Equal(0, snapshotStore.SaveCount);
    }

    [Fact]
    public async Task HandleCommandAsync_EventCountStrategy5_WritesAfterFiveCommands()
    {
        var snapshotStore = new RecordingDcbSnapshotStore();
        var resolver = new SnapshotStrategyResolver();
        resolver.RegisterForDcb<TestDcbEntity>(new EventCountSnapshotStrategy(5));
        var repo = CreateRepository(snapshotStore, resolver);

        var entity = await repo.CreateEntityAsync<TestDcbEntity>(new[] { "order:snap-5b" });
        var metadata = new CommandMetadata { UserId = "u" };
        for (var i = 0; i < 4; i++)
            entity = await repo.HandleCommandAsync(entity, new TestCommand { Value = $"e{i}" }, metadata);

        Assert.Equal(4, entity.EventsSinceLastSnapshot);
        Assert.Equal(0, snapshotStore.SaveCount);

        entity = await repo.HandleCommandAsync(entity, new TestCommand { Value = "e4" }, metadata);
        await snapshotStore.WaitForSaveAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(1, snapshotStore.SaveCount);
        Assert.Equal(0, entity.EventsSinceLastSnapshot);
    }

    [Fact]
    public async Task HandleCommandAsync_EventCountStrategy5_CountsReplayedEventsOnReload()
    {
        var tags = new[] { "order:snap-reload" };
        var seedRepo = _repository;
        var entity = await seedRepo.CreateEntityAsync<TestDcbEntity>(tags);
        var metadata = new CommandMetadata { UserId = "u" };
        for (var i = 0; i < 3; i++)
            entity = await seedRepo.HandleCommandAsync(entity, new TestCommand { Value = $"seed{i}" }, metadata);

        var snapshotStore = new RecordingDcbSnapshotStore();
        var resolver = new SnapshotStrategyResolver();
        resolver.RegisterForDcb<TestDcbEntity>(new EventCountSnapshotStrategy(5));
        var repo = CreateRepository(snapshotStore, resolver);

        var loaded = await repo.GetOrCreateEntityAsync<TestDcbEntity>(tags);
        Assert.Equal(3, loaded.EventsSinceLastSnapshot);

        loaded = await repo.HandleCommandAsync(loaded, new TestCommand { Value = "four" }, metadata);
        Assert.Equal(4, loaded.EventsSinceLastSnapshot);
        Assert.Equal(0, snapshotStore.SaveCount);

        loaded = await repo.HandleCommandAsync(loaded, new TestCommand { Value = "five" }, metadata);
        await snapshotStore.WaitForSaveAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(1, snapshotStore.SaveCount);
    }

    private DcbRepository CreateRepository(
        IDcbSnapshotStore snapshotStore,
        ISnapshotStrategyResolver resolver)
        => new(
            _eventStore,
            _metadataProvider,
            _tenantProvider,
            Options.Create(new LawnDartOptions()),
            null,
            null,
            NullLogger<DcbRepository>.Instance,
            null,
            snapshotStore,
            resolver);

    private sealed class RecordingDcbSnapshotStore : IDcbSnapshotStore
    {
        private int _saves;
        private readonly TaskCompletionSource<bool> _saved = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int SaveCount => Volatile.Read(ref _saves);

        public Task<(TState? State, SnapshotInfo? Info)> LoadDcbSnapshotAsync<TState>(
            string dcbId,
            CancellationToken ct = default)
            => Task.FromResult<(TState?, SnapshotInfo?)>((default, null));

        public string? LastDcbId { get; private set; }
        public IReadOnlyList<string>? LastLoadTags { get; private set; }

        public Task SaveDcbSnapshotAsync<TState>(
            string dcbId,
            long globalSequence,
            TState state,
            byte[]? consistencyMarker = null,
            IReadOnlyList<string>? loadTags = null,
            CancellationToken ct = default)
        {
            LastDcbId = dcbId;
            LastLoadTags = loadTags;
            Interlocked.Increment(ref _saves);
            _saved.TrySetResult(true);
            return Task.CompletedTask;
        }

        public async Task WaitForSaveAsync(TimeSpan timeout)
        {
            var finished = await Task.WhenAny(_saved.Task, Task.Delay(timeout)).ConfigureAwait(false);
            if (finished != _saved.Task)
                throw new TimeoutException("DCB snapshot save was not observed.");
            await _saved.Task.ConfigureAwait(false);
        }
    }
}

// Test types

// Test types
public class TestState : IState
{
    public int EventCount { get; set; }
    public List<string> Values { get; set; } = new();

    public void ApplyEventToState(IEvent @event)
    {
        if (@event is TestEvent testEvent)
        {
            EventCount++;
            Values.Add(testEvent.Value);
        }
    }
}

public class TestEvent : IEvent
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public string Value { get; init; } = string.Empty;
}

public class TestCommand : ICommand
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public string Value { get; init; } = string.Empty;
}

public class TestDcbEntity : DcbEntity<TestState>
{
    public override Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
    {
        if (command is TestCommand testCommand)
        {
            Emit(new TestEvent { Value = testCommand.Value }, Tags.ToArray());
        }
        return Task.CompletedTask;
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        State.ApplyEventToState(@event);
    }
}

public class TestTagProvider : ITagProvider
{
    private readonly string[] _additionalTags;

    public TestTagProvider(string[] additionalTags)
    {
        _additionalTags = additionalTags;
    }

    public IEnumerable<string> GetTags(IEvent @event, object? context = null)
    {
        return _additionalTags;
    }
}

public class TestMetadataProvider : IMetadataProvider
{
    public CommandMetadata CaptureCommandMetadata(object? context = null)
    {
        return new CommandMetadata
        {
            UserId = "test-user",
            Timestamp = DateTime.UtcNow,
            CorrelationId = Guid.NewGuid().ToString()
        };
    }

    public EventMetadata EnrichEventMetadata(EventMetadata eventMetadata, CommandMetadata commandMetadata, IEvent @event)
    {
        eventMetadata.UserId = commandMetadata.UserId;
        eventMetadata.CorrelationId = commandMetadata.CorrelationId;
        eventMetadata.CausationId = commandMetadata.CorrelationId;
        eventMetadata.Timestamp = DateTime.UtcNow;
        return eventMetadata;
    }
}

public class TestTenantContextProvider : ITenantContextProvider
{
    private readonly string _tenantId;

    public TestTenantContextProvider(string tenantId)
    {
        _tenantId = tenantId;
    }

    public string? GetTenantId() => _tenantId;
    public string GetTenantIdRequired() => _tenantId;
}
