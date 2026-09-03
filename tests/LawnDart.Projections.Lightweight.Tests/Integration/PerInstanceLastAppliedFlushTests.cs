using System.Text.Json;
using LawnDart.EventSourcing.EventStore;
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
/// View checkpoint column stores per-instance last-applied sequence, not flush-wave position.
/// </summary>
[Trait("Category", "Integration")]
public sealed class PerInstanceLastAppliedFlushTests
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

    [Fact]
    public async Task Flush_PersistsPerInstanceLastApplied_NotWavePosition()
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        // Two streams: early event on A, later event on B. After catch-up flush, A's stored
        // checkpoint must be A's apply seq — not B's (wave) position.
        await store.AppendAsync(
            "tenant1:Counter:early",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
            metadata: new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "test" },
            cancellationToken: cts.Token);

        // Pad with unrelated stream events so global positions diverge meaningfully.
        for (var i = 0; i < 5; i++)
        {
            await store.AppendAsync(
                $"tenant1:Other:pad-{i}",
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "test" },
                cancellationToken: cts.Token);
        }

        await store.AppendAsync(
            "tenant1:Counter:late",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 9)],
            metadata: new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "test" },
            cancellationToken: cts.Token);

        var head = await store.GetCurrentSequenceAsync(cts.Token);
        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(15),
            CheckpointInterval = 1000,
            BatchSize = 100,
            SkipTailFlushWhileCatchingUp = false
        };

        var runner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), options);

        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, "CounterSummary:v1", head, TimeSpan.FromSeconds(30), cts.Token);
        await runner.StopAsync(cts.Token);
        runner.Dispose();

        var early = await views.GetViewWithCheckpointAsync(
            "CounterSummary:v1", "tenant1:Counter:early", cts.Token);
        var late = await views.GetViewWithCheckpointAsync(
            "CounterSummary:v1", "tenant1:Counter:late", cts.Token);

        Assert.NotNull(early);
        Assert.NotNull(late);
        Assert.True(early!.Value.Checkpoint < late!.Value.Checkpoint,
            $"Expected early apply seq ({early.Value.Checkpoint}) < late ({late.Value.Checkpoint})");
        Assert.True(late.Value.Checkpoint <= head);

        Assert.Equal(1, JsonSerializer.Deserialize<CounterView>(early.Value.ViewData, ProjectionViewJson.Read)!.Count);
        Assert.Equal(9, JsonSerializer.Deserialize<CounterView>(late.Value.ViewData, ProjectionViewJson.Read)!.Count);

        // Global runner checkpoint may equal head (wave), while early instance stays behind.
        var global = await checkpoints.GetCheckpointAsync("CounterSummary:v1", 0, cts.Token);
        Assert.NotNull(global);
        Assert.True(global!.LastSequencePosition >= late.Value.Checkpoint);
        Assert.True(early.Value.Checkpoint < global.LastSequencePosition
                    || early.Value.Checkpoint == late.Value.Checkpoint); // degenerate if store seq odd
    }
}
