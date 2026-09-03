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
/// C1 — a throwing <c>Handle</c> retries then halts; the global checkpoint does not skip the event.
/// </summary>
public sealed class PoisonProjectionEventTests
{
    private const string StorageKey = "PoisonTagIndex:v1";

    [Fact]
    public async Task ApplyEvent_HandlerThrows_RetriesThenHaltsWithoutAdvancingCheckpoint()
    {
        PoisonTagIndexProjection.PoisonHandleCalls = 0;

        var store = new InMemoryEventStore();
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(10),
            CheckpointInterval = 1,
            BatchSize = 10,
            SkipTailFlushWhileCatchingUp = false
        };

        var good = new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "kept");
        await store.AppendAsync("system:Tags:1", [good], metadata: Meta());
        var goodSeq = await store.GetCurrentSequenceAsync();

        var reg = ProjectionScanner.TryBuildRegistration(typeof(PoisonTagIndexProjection))!;
        var runner = new LightweightProjectionRunnerService(
            reg, store, views, checkpoints, new SingleNodePartitioningService(), options);
        await runner.StartAsync(CancellationToken.None);

        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            StorageKey,
            ProjectionInstanceIds.UnpartitionedBase,
            TimeSpan.FromSeconds(10),
            json => JsonSerializer.Deserialize<GlobalIndexView>(json, ProjectionViewJson.Read)?.TotalTags == 1);
        Assert.NotNull(viewJson);

        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, StorageKey, goodSeq, TimeSpan.FromSeconds(10));

        var poison = new PoisonPill(Guid.NewGuid(), DateTime.UtcNow);
        await store.AppendAsync("system:Tags:1", [poison], metadata: Meta());
        var poisonSeq = await store.GetCurrentSequenceAsync();

        var later = new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "skipped");
        await store.AppendAsync("system:Tags:1", [later], metadata: Meta());

        await WaitForFaultedAsync(runner, TimeSpan.FromSeconds(10));

        var snapshot = runner.GetObservabilitySnapshot();
        Assert.True(snapshot.IsFaulted);
        Assert.True(
            snapshot.LastAppliedSequence < poisonSeq,
            $"Cursor {snapshot.LastAppliedSequence} skipped poison sequence {poisonSeq}.");

        var checkpoint = await checkpoints.GetCheckpointAsync(StorageKey, 0);
        Assert.NotNull(checkpoint);
        Assert.True(
            checkpoint!.LastSequencePosition < poisonSeq,
            $"Checkpoint {checkpoint.LastSequencePosition} skipped poison sequence {poisonSeq}.");

        var saved = await views.GetViewWithCheckpointAsync(StorageKey, ProjectionInstanceIds.UnpartitionedBase);
        Assert.NotNull(saved);
        var view = JsonSerializer.Deserialize<GlobalIndexView>(saved.Value.ViewData, ProjectionViewJson.Read);
        Assert.NotNull(view);
        Assert.Equal(1, view!.TotalTags);
        Assert.Equal(["kept"], view.Tags);

        Assert.Equal(
            LightweightProjectionRunnerService.PoisonRetryAttempts,
            PoisonTagIndexProjection.PoisonHandleCalls);

        await runner.StopAsync(CancellationToken.None);
    }

    private static EventMetadata Meta() =>
        new() { Timestamp = DateTime.UtcNow, UserId = "poison" };

    private static async Task WaitForFaultedAsync(
        LightweightProjectionRunnerService runner,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!cts.IsCancellationRequested)
        {
            if (runner.GetObservabilitySnapshot().IsFaulted)
                return;
            try
            {
                await Task.Delay(10, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        throw new TimeoutException(
            $"Runner did not halt on poison event. LastApplied={runner.GetObservabilitySnapshot().LastAppliedSequence}, " +
            $"IsFaulted={runner.GetObservabilitySnapshot().IsFaulted}, " +
            $"HandleCalls={PoisonTagIndexProjection.PoisonHandleCalls}.");
    }
}
