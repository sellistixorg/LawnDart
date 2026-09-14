using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventSourcing.Snapshots;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Snapshots;
using LawnDart.Snapshots.Telemetry;
using LawnDart.TestUtilities;
using Xunit;

namespace LawnDart.EventSourcing.Tests.Snapshots;

public class SnapshotWriteDurabilityTests
{
    private static IOptions<LawnDartOptions> CreateOptions() =>
        Microsoft.Extensions.Options.Options.Create(new LawnDartOptions { RequireTenantId = false });

    private static AggregateRepository CreateRepository(
        ISnapshotStore store,
        ISnapshotStrategyResolver resolver,
        ISnapshotWriteQueue queue,
        IEventStore? eventStore = null)
        => new(
            eventStore ?? new InMemoryEventStore(),
            new DefaultMetadataProvider(),
            new TestTenantContextProvider(),
            CreateOptions(),
            tagProvider: null,
            authorizationService: null,
            logger: null,
            snapshotStore: store,
            strategyResolver: resolver,
            snapshotWriteQueue: queue);

    [Fact]
    public async Task HandleCommandAsync_ThenMutateLiveAggregate_SnapshotMatchesCommittedVersion()
    {
        await using var pump = new SnapshotWritePump();
        await pump.StartAsync();

        var store = new MemorySnapshotStore();
        var resolver = new SnapshotStrategyResolver();
        resolver.RegisterForAggregate<ProbeAggregate>(new EventCountSnapshotStrategy(1));
        var repo = CreateRepository(store, resolver, pump.Queue);

        var aggregate = await repo.GetOrCreateAsync<ProbeAggregate>(Guid.NewGuid());
        await repo.HandleCommandAsync(aggregate, new SetValueCommand(Guid.NewGuid(), 7));
        aggregate.State.Value = 999;

        await pump.DisposeAsync();

        var (state, info) = await store.LoadSnapshotAsync<ProbeState>(aggregate.StreamId);
        Assert.NotNull(info);
        Assert.NotNull(state);
        Assert.Equal(7, state!.Value);
        Assert.Equal(aggregate.CommittedVersion, info!.Version);
    }

    [Fact]
    public async Task StoreFailure_IncrementsMetric_AndDegradesHealth()
    {
        await using var pump = new SnapshotWritePump();
        await pump.StartAsync();

        var failures = 0L;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == SnapshotTelemetry.SourceName &&
                instrument.Name == "snapshot.write_failures_total")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) =>
            Interlocked.Add(ref failures, value));
        listener.Start();

        var resolver = new SnapshotStrategyResolver();
        resolver.RegisterForAggregate<ProbeAggregate>(new EventCountSnapshotStrategy(1));
        var repo = CreateRepository(new ThrowingSnapshotStore(), resolver, pump.Queue);

        var aggregate = await repo.GetOrCreateAsync<ProbeAggregate>(Guid.NewGuid());
        await repo.HandleCommandAsync(aggregate, new SetValueCommand(Guid.NewGuid(), 1));

        await pump.DisposeAsync();

        Assert.True(pump.Queue.FailureCount >= 1);
        Assert.True(Volatile.Read(ref failures) >= 1);

        var health = new SnapshotWriteHealthCheck(pump.Queue);
        var result = await health.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Degraded, result.Status);
    }

    [Fact]
    public async Task Shutdown_DrainsInFlightWrites()
    {
        var store = new GateSnapshotStore();
        await using var pump = new SnapshotWritePump();
        await pump.StartAsync();

        var resolver = new SnapshotStrategyResolver();
        resolver.RegisterForAggregate<ProbeAggregate>(new EventCountSnapshotStrategy(1));
        var repo = CreateRepository(store, resolver, pump.Queue);

        var aggregate = await repo.GetOrCreateAsync<ProbeAggregate>(Guid.NewGuid());
        await repo.HandleCommandAsync(aggregate, new SetValueCommand(Guid.NewGuid(), 3));
        await store.Entered.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var stop = pump.DisposeAsync();
        store.Release.TrySetResult();
        await stop;

        Assert.Equal(1, store.SaveCount);
        var (state, _) = await store.LoadSnapshotAsync<ProbeState>(aggregate.StreamId);
        Assert.Equal(3, state!.Value);
    }

    [Fact]
    public async Task FullChannel_DoesNotBlockCaller_DropsOldest_IncrementsDropCounter()
    {
        var store = new GateSnapshotStore();
        await using var pump = new SnapshotWritePump(capacity: 1);
        await pump.StartAsync();

        var resolver = new SnapshotStrategyResolver();
        resolver.RegisterForAggregate<ProbeAggregate>(new EventCountSnapshotStrategy(1));
        var repo = CreateRepository(store, resolver, pump.Queue);

        var aggregate = await repo.GetOrCreateAsync<ProbeAggregate>(Guid.NewGuid());
        await repo.HandleCommandAsync(aggregate, new SetValueCommand(Guid.NewGuid(), 1));
        await store.Entered.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var sw = Stopwatch.StartNew();
        await repo.HandleCommandAsync(aggregate, new SetValueCommand(Guid.NewGuid(), 2));
        await repo.HandleCommandAsync(aggregate, new SetValueCommand(Guid.NewGuid(), 3));
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromMilliseconds(250),
            $"Enqueue blocked the caller for {sw.Elapsed}.");
        Assert.True(pump.Queue.DroppedCount >= 1);

        var health = new SnapshotWriteHealthCheck(pump.Queue);
        var result = await health.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Degraded, result.Status);

        store.Release.TrySetResult();
        await pump.DisposeAsync();

        var (state, _) = await store.LoadSnapshotAsync<ProbeState>(aggregate.StreamId);
        Assert.Equal(3, state!.Value);
    }

    [Fact]
    public async Task DropPath_DoesNotThrowToCaller()
    {
        var store = new GateSnapshotStore();
        await using var pump = new SnapshotWritePump(capacity: 1);
        await pump.StartAsync();

        var resolver = new SnapshotStrategyResolver();
        resolver.RegisterForAggregate<ProbeAggregate>(new EventCountSnapshotStrategy(1));
        var repo = CreateRepository(store, resolver, pump.Queue);

        var aggregate = await repo.GetOrCreateAsync<ProbeAggregate>(Guid.NewGuid());
        await repo.HandleCommandAsync(aggregate, new SetValueCommand(Guid.NewGuid(), 1));
        await store.Entered.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var ex = await Record.ExceptionAsync(() =>
            repo.HandleCommandAsync(aggregate, new SetValueCommand(Guid.NewGuid(), 2)));
        Assert.Null(ex);

        store.Release.TrySetResult();
        await pump.DisposeAsync();
    }

    private sealed class ProbeState : IState
    {
        public int Value { get; set; }
    }

    private sealed class ProbeAggregate : AggregateRoot<ProbeState>
    {
        public void Handle(SetValueCommand command) =>
            Apply(new ValueSet(Guid.NewGuid(), DateTime.UtcNow, command.Value));

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is ValueSet set)
                State.Value = set.Value;
        }
    }

    private sealed record SetValueCommand(Guid Id, int Value) : ICommand;

    [EventTypeName("snapshot-durability.value-set")]
    private sealed record ValueSet(Guid Id, DateTime Timestamp, int Value) : IEvent;

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

    private sealed class ThrowingSnapshotStore : ISnapshotStore
    {
        public Task<(TState? State, SnapshotInfo? Info)> LoadSnapshotAsync<TState>(
            string streamId, CancellationToken ct = default)
            => Task.FromResult<(TState?, SnapshotInfo?)>((default, null));

        public Task SaveSnapshotAsync<TState>(
            string streamId, long version, long globalSequence, TState state, CancellationToken ct = default)
            => throw new InvalidOperationException("snapshot store unavailable");
    }

    private sealed class GateSnapshotStore : ISnapshotStore
    {
        private readonly Dictionary<string, (object State, SnapshotInfo Info)> _snaps = new();
        private int _saves;

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int SaveCount => Volatile.Read(ref _saves);

        public Task<(TState? State, SnapshotInfo? Info)> LoadSnapshotAsync<TState>(
            string streamId, CancellationToken ct = default)
        {
            if (_snaps.TryGetValue(streamId, out var entry) && entry.State is TState state)
                return Task.FromResult<(TState?, SnapshotInfo?)>((state, entry.Info));
            return Task.FromResult<(TState?, SnapshotInfo?)>((default, null));
        }

        public async Task SaveSnapshotAsync<TState>(
            string streamId, long version, long globalSequence, TState state, CancellationToken ct = default)
        {
            Entered.TrySetResult();
            await Release.Task.WaitAsync(ct).ConfigureAwait(false);
            _snaps[streamId] = (state!, new SnapshotInfo(version, globalSequence, DateTime.UtcNow));
            Interlocked.Increment(ref _saves);
        }
    }
}
