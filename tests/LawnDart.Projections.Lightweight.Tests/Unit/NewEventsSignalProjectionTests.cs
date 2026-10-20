using System.Diagnostics;
using System.Text.Json;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public class NewEventsSignalProjectionTests
{
    [Fact]
    public async Task LiveView_WakesBeforeThePollInterval()
    {
        var store = new InMemoryEventStore();
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromSeconds(30),
            CheckpointInterval = 1,
            BatchSize = 10,
            UseSubscribe = false,
            SkipTailFlushWhileCatchingUp = false
        };

        var reg = ProjectionScanner.TryBuildRegistration(typeof(CounterSummaryProjection))!;
        var runner = new LightweightProjectionRunnerService(
            reg,
            store,
            views,
            checkpoints,
            new SingleNodePartitioningService(),
            options);

        await runner.StartAsync(CancellationToken.None);
        try
        {
            await WaitForWaitersAsync(store.NewEvents, 1);

            var started = Stopwatch.StartNew();
            const string streamId = "tenant1:Counter:wake";
            await store.AppendAsync(streamId, [
                new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 4)
            ], metadata: new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "test" });

            var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
                views,
                reg.StorageKey,
                streamId,
                TimeSpan.FromSeconds(2),
                json => JsonSerializer.Deserialize<CounterView>(json, ProjectionViewJson.Read)?.Count == 4);

            Assert.True(started.Elapsed < TimeSpan.FromSeconds(2), $"view landed after {started.Elapsed}");
            Assert.NotNull(viewJson);
        }
        finally
        {
            await runner.StopAsync(CancellationToken.None);
        }
    }

    private static async Task WaitForWaitersAsync(NewEventsSignal signal, int count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (signal.WaitingCount < count)
            await Task.Delay(10, cts.Token);
    }
}
