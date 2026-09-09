using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Snapshots;
using LawnDart.TestUtilities;
using Xunit;

namespace LawnDart.EventSourcing.Tests.Aggregates;

public class AggregateRepositoryTests
{
    private static IOptions<LawnDartOptions> CreateOptions() => 
        Options.Create(new LawnDartOptions { RequireTenantId = false });

    [Fact]
    public async Task GetAsync_NonExistentAggregate_ReturnsNull()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new DefaultMetadataProvider();
        var tenantContextProvider = new TestTenantContextProvider();
        var repository = new AggregateRepository(eventStore, metadataProvider, tenantContextProvider, CreateOptions());

        // Act
        var result = await repository.GetAsync<TestAggregate>(Guid.NewGuid());

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task GetAsync_ExistingAggregate_LoadsAndReplaysEvents()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new DefaultMetadataProvider();
        var tenantContextProvider = new TestTenantContextProvider();
        var repository = new AggregateRepository(eventStore, metadataProvider, tenantContextProvider, CreateOptions());

        var aggregateId = Guid.NewGuid();
        var streamId = $"test-tenant:TestAggregate:{aggregateId}";
        var events = new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        };

        await eventStore.AppendAsync(streamId, events);

        // Act
        var result = await repository.GetAsync<TestAggregate>(aggregateId);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(2, result!.Version);
        Assert.Equal(2, result.AppliedEventCount);
    }

    [Fact]
    public async Task SaveAsync_PersistsPendingEvents()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new DefaultMetadataProvider();
        var tenantContextProvider = new TestTenantContextProvider();
        var repository = new AggregateRepository(eventStore, metadataProvider, tenantContextProvider, CreateOptions());

        var aggregate = new TestAggregate();
        aggregate.SetStreamId($"test-tenant:TestAggregate:{Guid.NewGuid()}");
        aggregate.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));

        var commandMetadata = new CommandMetadata
        {
            TenantId = "test-tenant",
            UserId = "user123",
            CorrelationId = "corr-123"
        };

        // Act
        await repository.SaveAsync(aggregate, commandMetadata);

        // Assert
        Assert.Empty(aggregate.PendingEvents);
        var events = await eventStore.ReadStreamAsync(aggregate.StreamId);
        Assert.Single(events);
    }

    [Fact]
    public async Task SaveAsync_PropagatesMetadata()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new DefaultMetadataProvider();
        var tenantContextProvider = new TestTenantContextProvider("tenant456");
        var repository = new AggregateRepository(eventStore, metadataProvider, tenantContextProvider, CreateOptions());

        var aggregate = new TestAggregate();
        aggregate.SetStreamId($"tenant456:TestAggregate:{Guid.NewGuid()}");
        aggregate.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));

        var commandMetadata = new CommandMetadata
        {
            UserId = "user123",
            TenantId = "tenant456",
            CorrelationId = "corr-123"
        };

        // Act
        await repository.SaveAsync(aggregate, commandMetadata);

        // Assert
        var events = await eventStore.ReadStreamAsync(aggregate.StreamId);
        Assert.Single(events);
        Assert.Equal("user123", events[0].Metadata.UserId);
        Assert.Equal("tenant456", events[0].Metadata.TenantId);
        Assert.Equal("corr-123", events[0].Metadata.CorrelationId);
    }

    [Fact]
    public async Task GetOrCreateAsync_WhenNotExists_CreatesNew()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new DefaultMetadataProvider();
        var tenantContextProvider = new TestTenantContextProvider();
        var repository = new AggregateRepository(eventStore, metadataProvider, tenantContextProvider, CreateOptions());

        var aggregateId = Guid.NewGuid();

        // Act
        var result = await repository.GetOrCreateAsync<TestAggregate>(aggregateId);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(0, result.Version);
        Assert.Equal($"test-tenant:TestAggregate:{aggregateId}", result.StreamId);
        Assert.Empty(result.PendingEvents);
    }

    [Fact]
    public async Task GetOrCreateAsync_WhenExists_ReturnsExisting()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new DefaultMetadataProvider();
        var tenantContextProvider = new TestTenantContextProvider();
        var repository = new AggregateRepository(eventStore, metadataProvider, tenantContextProvider, CreateOptions());

        var aggregateId = Guid.NewGuid();
        var streamId = $"test-tenant:TestAggregate:{aggregateId}";
        var events = new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        };

        await eventStore.AppendAsync(streamId, events);

        // Act
        var result = await repository.GetOrCreateAsync<TestAggregate>(aggregateId);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(2, result!.Version);
        Assert.Equal(2, result.AppliedEventCount);
        Assert.Equal(streamId, result.StreamId);
    }

    [Fact]
    public async Task GetOrCreateAsync_ThenModify_ThenSave_HandlesConcurrency()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new DefaultMetadataProvider();
        var tenantContextProvider = new TestTenantContextProvider();
        var repository = new AggregateRepository(eventStore, metadataProvider, tenantContextProvider, CreateOptions());

        var aggregateId = Guid.NewGuid();

        // Create initial aggregate
        var aggregate1 = await repository.GetOrCreateAsync<TestAggregate>(aggregateId);
        aggregate1.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));
        await repository.SaveAsync(aggregate1, new CommandMetadata { TenantId = "test-tenant", UserId = "user1" });

        // Simulate concurrent access: both threads load at version 1
        var aggregate2 = await repository.GetOrCreateAsync<TestAggregate>(aggregateId); // Version = 1
        var aggregate3 = await repository.GetOrCreateAsync<TestAggregate>(aggregateId); // Version = 1

        // Thread 2 modifies and saves first
        aggregate2!.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));
        await repository.SaveAsync(aggregate2, new CommandMetadata { TenantId = "test-tenant", UserId = "user2" }); // ✅ Succeeds

        // Thread 3 tries to save (should fail - version mismatch)
        aggregate3!.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));

        // Assert
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => repository.SaveAsync(aggregate3, new CommandMetadata { TenantId = "test-tenant", UserId = "user3" }));
    }

    [Fact]
    public async Task CreateAsync_ThenCreateAsyncAgain_SecondFlushFails()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new DefaultMetadataProvider();
        var tenantContextProvider = new TestTenantContextProvider();
        var repository = new AggregateRepository(eventStore, metadataProvider, tenantContextProvider, CreateOptions());

        var aggregateId = Guid.NewGuid();

        // First creation and flush
        var aggregate1 = await repository.CreateAsync<TestAggregate>(aggregateId);
        aggregate1.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));
        await repository.SaveAsync(aggregate1, new CommandMetadata { TenantId = "test-tenant", UserId = "user1" });

        // Second attempt to "create" the same aggregate (incorrect usage, but tests the scenario)
        var aggregate2 = await repository.CreateAsync<TestAggregate>(aggregateId); // ❌ Doesn't check if exists
        aggregate2.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));

        // Assert - second flush should fail because aggregate already exists
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => repository.SaveAsync(aggregate2, new CommandMetadata { TenantId = "test-tenant", UserId = "user2" }));
    }

    [Fact]
    public async Task GetOrCreateAsync_ConcurrentCalls_OnlyOneCreates()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new DefaultMetadataProvider();
        var tenantContextProvider = new TestTenantContextProvider();
        var repository = new AggregateRepository(eventStore, metadataProvider, tenantContextProvider, CreateOptions());

        var aggregateId = Guid.NewGuid();

        // Simulate concurrent GetOrCreateAsync calls
        var task1 = repository.GetOrCreateAsync<TestAggregate>(aggregateId);
        var task2 = repository.GetOrCreateAsync<TestAggregate>(aggregateId);
        var task3 = repository.GetOrCreateAsync<TestAggregate>(aggregateId);

        var results = await Task.WhenAll(task1, task2, task3);

        // All should return aggregates with the same StreamId
        var streamId = $"test-tenant:TestAggregate:{aggregateId}";
        Assert.All(results, r =>
        {
            Assert.NotNull(r);
            Assert.Equal(streamId, r.StreamId);
        });

        // When we flush the first one, others should detect the conflict if they try to flush
        results[0].ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));
        await repository.SaveAsync(results[0], new CommandMetadata { TenantId = "test-tenant", UserId = "user1" });

        // Now if others try to flush, they should fail
        results[1].ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => repository.SaveAsync(results[1], new CommandMetadata { TenantId = "test-tenant", UserId = "user2" }));
    }

    [Fact]
    public async Task HandleMultipleCommands_WithConcurrentModification_ThrowsConcurrencyException()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new DefaultMetadataProvider();
        var tenantContextProvider = new TestTenantContextProvider();
        var repository = new AggregateRepository(eventStore, metadataProvider, tenantContextProvider, CreateOptions());

        var aggregateId = Guid.NewGuid();

        // Create initial aggregate with one item
        var initialAggregate = await repository.GetOrCreateAsync<CounterAggregate>(aggregateId);
        await initialAggregate.HandleAsync(new IncrementCommand(Guid.NewGuid(), 1));
        await repository.SaveAsync(initialAggregate, new CommandMetadata { TenantId = "test-tenant", UserId = "user1" });

        // Thread 1: Load aggregate and handle first command (but don't save yet)
        var thread1Aggregate = await repository.GetOrCreateAsync<CounterAggregate>(aggregateId);
        // Thread 1 thinks it's at version 1 (one event in store)
        Assert.Equal(1, thread1Aggregate.Version);
        
        await thread1Aggregate.HandleAsync(new IncrementCommand(Guid.NewGuid(), 5));
        // Thread 1 now has version 2 locally (pending event), but hasn't saved yet

        // Thread 2: Load same aggregate, handle a different command, and save successfully
        var thread2Aggregate = await repository.GetOrCreateAsync<CounterAggregate>(aggregateId);
        // Thread 2 also loads at version 1
        Assert.Equal(1, thread2Aggregate.Version);
        
        await thread2Aggregate.HandleAsync(new IncrementCommand(Guid.NewGuid(), 10));
        await repository.SaveAsync(thread2Aggregate, new CommandMetadata { TenantId = "test-tenant", UserId = "user2" });
        // Thread 2 successfully saves, aggregate is now at version 2 in store

        // Thread 1: Now handles a second command (still using stale state from initial load)
        // This is the problematic pattern: handling multiple commands before saving
        await thread1Aggregate.HandleAsync(new IncrementCommand(Guid.NewGuid(), 3));
        // Thread 1 thinks it's at version 3 locally (1 initial + 2 pending events)
        // But the store is actually at version 2 (1 initial + 1 from thread 2)

        // Assert: Thread 1's save should fail with ConcurrencyException
        // Expected version: 1 (version before thread1's pending events)
        // Actual version in store: 2 (because thread2 saved)
        var exception = await Assert.ThrowsAsync<ConcurrencyException>(
            () => repository.SaveAsync(thread1Aggregate, new CommandMetadata { TenantId = "test-tenant", UserId = "user1" }));

        Assert.NotNull(exception);
        Assert.Equal(1L, exception.ExpectedPosition); // Thread 1 expected version 1
        Assert.Equal(2L, exception.ActualPosition);   // But store is at version 2

        // Verify final state: only thread2's change was persisted
        var finalAggregate = await repository.GetAsync<CounterAggregate>(aggregateId);
        Assert.NotNull(finalAggregate);
        Assert.Equal(2, finalAggregate!.Version);
        Assert.Equal(11, finalAggregate.State.Total); // Initial 1 + thread2's 10 = 11
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

    // ── CommittedVersion tests ────────────────────────────────────────────────

    [Fact]
    public async Task CommittedVersion_AfterCreateAsync_IsMinusOne()
    {
        var eventStore  = new InMemoryEventStore();
        var repository  = new AggregateRepository(eventStore, new DefaultMetadataProvider(), new TestTenantContextProvider(), CreateOptions());
        var aggregateId = Guid.NewGuid();

        var aggregate = await repository.CreateAsync<TestAggregate>(aggregateId);

        Assert.Equal(-1, aggregate.CommittedVersion);
    }

    [Fact]
    public async Task CommittedVersion_AfterGetAsync_MatchesStreamVersion()
    {
        var eventStore  = new InMemoryEventStore();
        var repository  = new AggregateRepository(eventStore, new DefaultMetadataProvider(), new TestTenantContextProvider(), CreateOptions());
        var aggregateId = Guid.NewGuid();
        var streamId    = $"test-tenant:TestAggregate:{aggregateId}";

        // Seed two events directly — store assigns versions 1 and 2
        await eventStore.AppendAsync(streamId, new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        });

        var aggregate = await repository.GetAsync<TestAggregate>(aggregateId);

        Assert.NotNull(aggregate);
        Assert.Equal(2, aggregate!.Version);
        Assert.Equal(2, aggregate.CommittedVersion);
    }

    [Fact]
    public async Task CommittedVersion_AfterSaveAsync_IsUpdated()
    {
        var eventStore  = new InMemoryEventStore();
        var repository  = new AggregateRepository(eventStore, new DefaultMetadataProvider(), new TestTenantContextProvider(), CreateOptions());
        var aggregateId = Guid.NewGuid();
        var cmd         = new CommandMetadata { TenantId = "test-tenant", UserId = "u1" };

        var aggregate = await repository.CreateAsync<TestAggregate>(aggregateId);
        Assert.Equal(-1, aggregate.CommittedVersion);   // -1 = brand new, never saved

        aggregate.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));
        await repository.SaveAsync(aggregate, cmd);

        Assert.Equal(aggregate.Version, aggregate.CommittedVersion);
        Assert.True(aggregate.CommittedVersion >= 0, "CommittedVersion should be non-negative after flush");
    }

    [Fact]
    public async Task CommittedVersion_MultipleFlushes_TracksLatestVersion()
    {
        var eventStore  = new InMemoryEventStore();
        var repository  = new AggregateRepository(eventStore, new DefaultMetadataProvider(), new TestTenantContextProvider(), CreateOptions());
        var aggregateId = Guid.NewGuid();
        var cmd         = new CommandMetadata { TenantId = "test-tenant", UserId = "u1" };

        var aggregate = await repository.CreateAsync<TestAggregate>(aggregateId);

        // First save
        aggregate.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));
        await repository.SaveAsync(aggregate, cmd);
        var afterFirst = aggregate.CommittedVersion;

        // Second save via GetOrCreate round-trip (reload to reset state, then append)
        var reloaded = await repository.GetAsync<TestAggregate>(aggregateId);
        Assert.NotNull(reloaded);
        reloaded!.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));
        await repository.SaveAsync(reloaded, cmd);

        Assert.True(reloaded.CommittedVersion > afterFirst);
        Assert.Equal(reloaded.Version, reloaded.CommittedVersion);
    }

    [Fact]
    public async Task GetAsync_ByStreamId_Missing_ReturnsNull()
    {
        var repository = new AggregateRepository(
            new InMemoryEventStore(), new DefaultMetadataProvider(), new TestTenantContextProvider(), CreateOptions());

        var result = await repository.GetAsync<TestAggregate>("42:InboundShipment:abc:SHIP-1");

        Assert.Null(result);
    }

    [Fact]
    public async Task GetOrCreateAsync_ByStreamId_WhenNotExists_SetsStreamIdAndCommittedVersionMinusOne()
    {
        var repository = new AggregateRepository(
            new InMemoryEventStore(), new DefaultMetadataProvider(), new TestTenantContextProvider(), CreateOptions());
        const string streamId = "42:InboundShipment:abc:SHIP-1";

        var result = await repository.GetOrCreateAsync<TestAggregate>(streamId);

        Assert.Equal(streamId, result.StreamId);
        Assert.Equal(-1, result.CommittedVersion);
        Assert.Equal(0, result.Version);
    }

    [Fact]
    public async Task GetOrCreateAsync_ByStreamId_WhenExists_LoadsEventsAndCommittedVersion()
    {
        var eventStore = new InMemoryEventStore();
        var repository = new AggregateRepository(
            eventStore, new DefaultMetadataProvider(), new TestTenantContextProvider(), CreateOptions());
        const string streamId = "42:InboundShipment:abc:SHIP-1";
        await eventStore.AppendAsync(streamId, new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        });

        var result = await repository.GetOrCreateAsync<TestAggregate>(streamId);

        Assert.Equal(streamId, result.StreamId);
        Assert.Equal(2, result.Version);
        Assert.Equal(2, result.CommittedVersion);
        Assert.Equal(2, result.AppliedEventCount);
    }

    [Fact]
    public async Task GetOrCreateAsync_ByStreamId_ThenSave_HandlesConcurrency()
    {
        var eventStore = new InMemoryEventStore();
        var repository = new AggregateRepository(
            eventStore, new DefaultMetadataProvider(), new TestTenantContextProvider(), CreateOptions());
        const string streamId = "42:InboundShipment:abc:SHIP-1";
        var cmd = new CommandMetadata { TenantId = "test-tenant", UserId = "user1" };

        var first = await repository.GetOrCreateAsync<TestAggregate>(streamId);
        first.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));
        await repository.SaveAsync(first, cmd);

        var a = await repository.GetOrCreateAsync<TestAggregate>(streamId);
        var b = await repository.GetOrCreateAsync<TestAggregate>(streamId);
        a.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));
        await repository.SaveAsync(a, cmd);
        b.ApplyTest(new TestEvent(Guid.NewGuid(), DateTime.UtcNow));

        await Assert.ThrowsAsync<ConcurrencyException>(() => repository.SaveAsync(b, cmd));
    }

    [Fact]
    public async Task GetAsync_ByStreamId_RestoresSnapshotThenReplaysDelta()
    {
        var eventStore = new InMemoryEventStore();
        var snapshots = new MemorySnapshotStore();
        const string streamId = "42:InboundShipment:abc:SHIP-1";
        await eventStore.AppendAsync(streamId, new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        });
        await snapshots.SaveSnapshotAsync(streamId, 2, 2, new TestState { EventCount = 2 });

        var repository = new AggregateRepository(
            eventStore, new DefaultMetadataProvider(), new TestTenantContextProvider(), CreateOptions(),
            snapshotStore: snapshots);

        var result = await repository.GetAsync<TestAggregate>(streamId);

        Assert.NotNull(result);
        Assert.Equal(streamId, result!.StreamId);
        Assert.Equal(3, result.Version);
        Assert.Equal(3, result.CommittedVersion);
        Assert.Equal(1, result.AppliedEventCount);
    }

    [Fact]
    public async Task GetAsync_ByStreamId_Empty_Throws()
    {
        var repository = new AggregateRepository(
            new InMemoryEventStore(), new DefaultMetadataProvider(), new TestTenantContextProvider(), CreateOptions());

        await Assert.ThrowsAsync<ArgumentException>(() => repository.GetAsync<TestAggregate>(" "));
    }

    // Test aggregate for command batching scenario
    private class CounterState : IState
    {
        public int Total { get; set; }
    }

    private class CounterAggregate : AggregateRoot<CounterState>
    {
        public CounterAggregate()
        {
            State = new CounterState();
        }

        public override Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
        {
            switch (command)
            {
                case IncrementCommand inc:
                    Apply(new IncrementedEvent(Guid.NewGuid(), DateTime.UtcNow, inc.Amount));
                    break;
            }
            return Task.CompletedTask;
        }

        protected override void ApplyEventToState(IEvent @event)
        {
            switch (@event)
            {
                case IncrementedEvent inc:
                    State.Total += inc.Amount;
                    break;
            }
        }
    }

    private record IncrementCommand(Guid Id, int Amount) : ICommand;
    private record IncrementedEvent(Guid Id, DateTime Timestamp, int Amount) : IEvent;

    private sealed class MemorySnapshotStore : ISnapshotStore
    {
        private readonly Dictionary<string, (object State, SnapshotInfo Info)> _snaps = new();

        public Task<(TState? State, SnapshotInfo? Info)> LoadSnapshotAsync<TState>(
            string streamId, CancellationToken ct = default)
        {
            if (_snaps.TryGetValue(streamId, out var entry) && entry.State is TState state)
                return Task.FromResult<(TState?, SnapshotInfo?)>((state, entry.Info));
            return Task.FromResult<(TState?, SnapshotInfo?)>((default, null));
        }

        public Task SaveSnapshotAsync<TState>(
            string streamId, long version, long globalSequence, TState state, CancellationToken ct = default)
        {
            _snaps[streamId] = (state!, new SnapshotInfo(version, globalSequence, DateTime.UtcNow));
            return Task.CompletedTask;
        }
    }
}

