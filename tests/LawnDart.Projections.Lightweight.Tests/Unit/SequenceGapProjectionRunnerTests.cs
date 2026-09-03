using System.Text.Json;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Fakes;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

/// <summary>
/// Regression tests for global-sequence gaps (SQL Server CACHE 1000 / similar) where events
/// exist after an empty range. The runner must walk the gap, not jump to head-1 and skip them.
/// </summary>
public class SequenceGapProjectionRunnerTests
{
    private static ProjectionRegistration GlobalRegistration() =>
        new(
            handlerType: typeof(GlobalTagIndexProjection),
            viewType: typeof(GlobalIndexView),
            projectionName: "GlobalTagIndex",
            kind: ProjectionKind.Global,
            tenantScope: TenantScope.SystemGlobal,
            endpoint: new ProjectionEndpointAttribute("/api/views/tags"));

    private static SequenceGapEventStore BuildStoreWithGapAfterFiveEvents()
    {
        var meta = new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "test" };
        const string streamId = "system:Tags:1";

        var events = new List<SequencedEvent>();
        for (long seq = 1; seq <= 5; seq++)
        {
            events.Add(new SequencedEvent(
                new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, $"early-{seq}"),
                seq,
                streamId,
                seq,
                meta));
        }

        for (long seq = 1002; seq <= 1004; seq++)
        {
            events.Add(new SequencedEvent(
                new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, $"after-gap-{seq}"),
                seq,
                streamId,
                seq,
                meta));
        }

        return new SequenceGapEventStore(events);
    }

    [Fact]
    public async Task GlobalProjection_ColdStart_ProcessesAllEventsAfterSequenceGap()
    {
        var store = BuildStoreWithGapAfterFiveEvents();
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        var options = new LightweightProjectionOptions
        {
            PollInterval       = TimeSpan.FromMilliseconds(10),
            CheckpointInterval = 100,
            BatchSize          = 100,
        };

        var runner = await ProjectionRunnerTestHarness.StartRunnerAsync(
            GlobalRegistration(),
            store,
            views,
            checkpoints,
            options,
            cts.Token);

        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "GlobalTagIndex:v1",
            "global",
            TimeSpan.FromSeconds(10),
            json =>
            {
                var view = JsonSerializer.Deserialize<GlobalIndexView>(json, ProjectionViewJson.Read);
                return view is not null && view.TotalTags == 8;
            });

        await runner.StopAsync(cts.Token);

        Assert.NotNull(viewJson);
        var finalView = JsonSerializer.Deserialize<GlobalIndexView>(viewJson!, ProjectionViewJson.Read)!;
        Assert.Equal(8, finalView.TotalTags);

        var checkpoint = await checkpoints.GetCheckpointAsync("GlobalTagIndex:v1", 0, cts.Token);
        Assert.NotNull(checkpoint);
        Assert.Equal(1004, checkpoint!.LastSequencePosition);
    }
}
