using System.Text.Json;
using LawnDart;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Fakes;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Lightweight.TimeTravelQuery;
using LawnDart.Projections.Storage;
using LawnDart.Projections.Checkpoints;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

/// <summary>
/// Regression tests for filtered MultiStream catch-up when
/// <see cref="LawnDart.EventStore.IEventStore.ReadByQueryStreamAsync"/> returns no rows
/// but <see cref="LawnDart.EventStore.IEventStore.GetCurrentSequenceAsync"/> has advanced.
/// </summary>
public class MultiStreamFilteredCatchUpRunnerTests
{
    private static ProjectionRegistration CatalogRegistration() =>
        ProjectionScanner.TryBuildRegistration(typeof(CatalogListingProjection))!;

    [Fact]
    public async Task MultiStream_WhenFirstFilteredReadEmptyButHeadIncludesSequence_StillProjectsEvent()
    {
        var store = new ControllableQueryEventStore { EmptyQueryReadsRemaining = 1 };
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        var listingId = Guid.NewGuid();
        var itemId    = Guid.NewGuid();

        var append = await store.AppendAsync(
            "acme:ListingAggregate:listing-a",
            [new CatalogItemAdded(Guid.NewGuid(), DateTime.UtcNow, listingId, itemId)],
            metadata: ProjectionRunnerTestHarness.MakeMeta(),
            cancellationToken: cts.Token);

        var targetSequence = append.SequencePositions[0];

        var runner = await ProjectionRunnerTestHarness.StartRunnerAsync(
            CatalogRegistration(),
            store,
            views,
            checkpoints,
            ProjectionRunnerTestHarness.AggressivePollOptions,
            cts.Token);

        var instanceKey = $"acme:{listingId}";
        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "CatalogListing:v1",
            instanceKey,
            TimeSpan.FromSeconds(10),
            json =>
            {
                var v = ProjectionRunnerTestHarness.DeserializeCatalogListing(json);
                return v is not null && v.ItemIds.Contains(itemId);
            });

        await runner.StopAsync(cts.Token);

        Assert.NotNull(viewJson);
        var view = ProjectionRunnerTestHarness.DeserializeCatalogListing(viewJson!)!;
        Assert.Contains(itemId, view.ItemIds);

        var checkpoint = await checkpoints.GetCheckpointAsync("CatalogListing:v1", 0, cts.Token);
        Assert.NotNull(checkpoint);
        Assert.True(checkpoint!.LastSequencePosition >= targetSequence);

        // Ad-hoc replay must agree with the live runner result.
        var builder = new AdHocProjectionBuilder(store.Inner, [CatalogRegistration()]);
        var headForReplay = await store.Inner.GetCurrentSequenceAsync(cts.Token);
        var replay = await builder.BuildAtAsync(
            "CatalogListing",
            instanceKey,
            ProjectionCutoff.AtSequence(headForReplay),
            cts.Token);
        var replayView = JsonSerializer.Deserialize<CatalogListingView>(replay.ViewJson, ProjectionViewJson.Read)!;
        Assert.Contains(itemId, replayView.ItemIds);
    }
}
