using System.Text.Json;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Barriered parallel flush: failure retains dirties / blocks checkpoint, catch-up policy.
/// </summary>
[Trait("Category", "Integration")]
public class FlushParallelismAndCatchUpTests
{
    private static ProjectionRegistration MakeCounterReg() =>
        new(
            handlerType: typeof(CounterSummaryProjection),
            viewType: typeof(CounterView),
            projectionName: "CounterSummary",
            kind: ProjectionKind.SingleStream,
            tenantScope: TenantScope.TenantScoped,
            streamType: "Counter",
            endpoint: new ProjectionEndpointAttribute("/api/views/counters/{id}", "Counter.View"));

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    [Fact]
    public async Task BulkFlush_SaveViewsAsync_PersistsAllDirtyViews()
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store = new LawnDart.EventSourcing.EventStore.InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        for (var i = 0; i < 16; i++)
        {
            await store.AppendAsync($"tenant1:Counter:p-{i}", [
                new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)
            ], metadata: Meta(), cancellationToken: cts.Token);
        }

        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(20),
            CheckpointInterval = 1000,
            BatchSize = 100,
            SkipTailFlushWhileCatchingUp = false
        };

        var runner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), options);

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, "CounterSummary:v1", 15, TimeSpan.FromSeconds(45));
        await runner.StopAsync(cts.Token);
        runner.Dispose();

        var all = (await views.GetViewsByTypeAsync("CounterSummary:v1", cts.Token)).ToList();
        Assert.Equal(16, all.Count);
    }

    [Fact]
    public async Task FlushFailure_DoesNotAdvanceCheckpoint_ThenRecovers()
    {
        var innerViews = new InMemoryViewStore();
        // Fail first 2 save attempts (covers first flush of 2 streams if sequential, or partial parallel)
        var views = new FailThenSucceedViewStore(innerViews, failAfter: 2);
        var checkpoints = new CountingCheckpointStore(new InMemoryCheckpointStore(innerViews));
        var store = new LawnDart.EventSourcing.EventStore.InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        await store.AppendAsync("tenant1:Counter:a", [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)
        ], metadata: Meta(), cancellationToken: cts.Token);
        await store.AppendAsync("tenant1:Counter:b", [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 2)
        ], metadata: Meta(), cancellationToken: cts.Token);

        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(30),
            CheckpointInterval = 1,
            BatchSize = 50,
            FlushMaxDegreeOfParallelism = 1, // deterministic fail-then-retry ordering
            SkipTailFlushWhileCatchingUp = false
        };

        var runner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), options);

        await runner.StartAsync(CancellationToken.None);

        // Wait until a successful checkpoint exists (recovery after injected failures)
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, "CounterSummary:v1", 1, TimeSpan.FromSeconds(45));

        await runner.StopAsync(cts.Token);
        runner.Dispose();

        Assert.True(views.Attempts > 2, "Expected retries after injected failures");
        Assert.True(checkpoints.SaveCount >= 1);

        var viewA = await innerViews.GetViewAsync("CounterSummary:v1", "tenant1:Counter:a", cts.Token);
        var viewB = await innerViews.GetViewAsync("CounterSummary:v1", "tenant1:Counter:b", cts.Token);
        Assert.NotNull(viewA);
        Assert.NotNull(viewB);
        Assert.Equal(1, JsonSerializer.Deserialize<CounterView>(viewA!, ProjectionViewJson.Read)!.Count);
        Assert.Equal(2, JsonSerializer.Deserialize<CounterView>(viewB!, ProjectionViewJson.Read)!.Count);
    }

    [Fact]
    public async Task SequentialFlush_Dop1_StillPersistsAllViews()
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store = new LawnDart.EventSourcing.EventStore.InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        for (var i = 0; i < 5; i++)
        {
            await store.AppendAsync($"tenant1:Counter:s-{i}", [
                new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, i + 1)
            ], metadata: Meta(), cancellationToken: cts.Token);
        }

        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(20),
            CheckpointInterval = 1,
            BatchSize = 50,
            FlushMaxDegreeOfParallelism = 1,
            SkipTailFlushWhileCatchingUp = false
        };

        var runner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), options);

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, "CounterSummary:v1", 4, TimeSpan.FromSeconds(15), cts.Token);
        await runner.StopAsync(cts.Token);
        runner.Dispose();

        for (var i = 0; i < 5; i++)
        {
            var json = await views.GetViewAsync("CounterSummary:v1", $"tenant1:Counter:s-{i}", cts.Token);
            Assert.NotNull(json);
            Assert.Equal(i + 1, JsonSerializer.Deserialize<CounterView>(json!, ProjectionViewJson.Read)!.Count);
        }
    }
}
