using System.Text.Json;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Lightweight.TimeTravelQuery;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Stress tests for live <see cref="Hosting.LightweightProjectionRunnerService"/> polling
/// while events are being appended concurrently.
/// </summary>
[Trait("Category", "Integration")]
public class MultiStreamLivePollStressTests
{
    private static ProjectionRegistration CatalogRegistration() =>
        ProjectionScanner.TryBuildRegistration(typeof(CatalogListingProjection))!;

    [Fact]
    public async Task MultiStream_ConcurrentAppends_ViewEventuallyMatchesAdHocReplay()
    {
        var views       = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store       = new LawnDart.EventSourcing.EventStore.InMemoryEventStore();

        var listingId = Guid.NewGuid();
        var itemIds   = Enumerable.Range(0, 20).Select(_ => Guid.NewGuid()).ToArray();

        var runner = await ProjectionRunnerTestHarness.StartRunnerAsync(
            CatalogRegistration(),
            store,
            views,
            checkpoints,
            ProjectionRunnerTestHarness.AggressivePollOptions);

        var writer = Task.Run(async () =>
        {
            for (var i = 0; i < itemIds.Length; i++)
            {
                var stream = i % 2 == 0
                    ? $"acme:ListingAggregate:{listingId}"
                    : $"acme:ListingInventory:{Guid.NewGuid()}";

                await store.AppendAsync(
                    stream,
                    [new CatalogItemAdded(Guid.NewGuid(), DateTime.UtcNow, listingId, itemIds[i])],
                    metadata: ProjectionRunnerTestHarness.MakeMeta(),
                    cancellationToken: CancellationToken.None);

                if (i % 3 == 2)
                {
                    var removeId = itemIds[i - 1];
                    await store.AppendAsync(
                        $"acme:ListingAggregate:{listingId}",
                        [new CatalogItemRemoved(Guid.NewGuid(), DateTime.UtcNow, listingId, removeId)],
                        metadata: ProjectionRunnerTestHarness.MakeMeta(),
                        cancellationToken: CancellationToken.None);
                }

                await Task.Delay(5, CancellationToken.None);
            }
        });

        await writer;

        var instanceKey = $"acme:{listingId}";
        var head        = await store.GetCurrentSequenceAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints,
            "CatalogListing:v1",
            head,
            TimeSpan.FromSeconds(60));

        var builder     = new AdHocProjectionBuilder(store, [CatalogRegistration()]);
        var replay      = await builder.BuildAtAsync(
            "CatalogListing",
            instanceKey,
            ProjectionCutoff.AtSequence(head),
            CancellationToken.None);
        var expected    = JsonSerializer.Deserialize<CatalogListingView>(replay.ViewJson, ProjectionViewJson.Read)!;

        string? viewJson = null;
        using (var waitCts = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
        {
            while (!waitCts.IsCancellationRequested)
            {
                viewJson = await views.GetViewAsync("CatalogListing:v1", instanceKey, CancellationToken.None);
                if (viewJson is not null)
                {
                    var live = ProjectionRunnerTestHarness.DeserializeCatalogListing(viewJson)!;
                    if (live.ItemIds.OrderBy(x => x).SequenceEqual(expected.ItemIds.OrderBy(x => x))
                        && live.ProcessedEventCount == expected.ProcessedEventCount)
                        break;
                }

                try
                {
                    await Task.Delay(25, waitCts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        await runner.StopAsync(CancellationToken.None);

        Assert.NotNull(viewJson);
        var liveView = ProjectionRunnerTestHarness.DeserializeCatalogListing(viewJson!)!;

        Assert.Equal(expected.ListingId, liveView.ListingId);
        Assert.Equal(
            expected.ItemIds.OrderBy(x => x),
            liveView.ItemIds.OrderBy(x => x));
        Assert.Equal(expected.ProcessedEventCount, liveView.ProcessedEventCount);
    }
}
