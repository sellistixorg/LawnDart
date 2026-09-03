using System.Text.Json;
using LawnDart;
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
/// Integration tests for <see cref="LightweightProjectionRunnerService"/> with
/// <see cref="ProjectionKind.Dcb"/> and a filtered event-type query.
/// </summary>
[Trait("Category", "Integration")]
public class DcbProjectionRunnerIntegrationTests
{
    private static readonly LightweightProjectionOptions DefaultOptions = new()
    {
        PollInterval         = TimeSpan.FromMilliseconds(50),
        CheckpointInterval   = 10,
        BatchSize            = 100
    };

    private static ProjectionRegistration MakeDcbRegistration() =>
        ProjectionScanner.TryBuildRegistration(typeof(CounterDcbFeedProjection))!;

    private static (IEventStore store, IViewStore views, ICheckpointStore checkpoints)
        BuildInMemoryStores()
    {
        var views       = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store       = new LawnDart.EventSourcing.EventStore.InMemoryEventStore();
        return (store, views, checkpoints);
    }

    private static EventMetadata MakeMeta() =>
        new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static async Task<LightweightProjectionRunnerService> StartRunnerAsync(
        ProjectionRegistration reg,
        IEventStore eventStore,
        IViewStore views,
        ICheckpointStore checkpoints,
        CancellationToken ct)
    {
        _ = ct;
        var partitioning = new SingleNodePartitioningService();
        var runner = new LightweightProjectionRunnerService(
            reg, eventStore, views, checkpoints, partitioning, DefaultOptions);
        await runner.StartAsync(CancellationToken.None);
        return runner;
    }

    [Fact]
    public async Task Dcb_Runner_ProcessesSingleCounterEvent_WithoutSparsePrefix()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await store.AppendAsync(
            "any:Counter:one",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 42)],
            metadata: MakeMeta(),
            cancellationToken: cts.Token);

        var runner = await StartRunnerAsync(
            MakeDcbRegistration(), store, views, checkpoints, cts.Token);

        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "CounterDcbFeed:v1",
            "global",
            TimeSpan.FromSeconds(30),
            json => JsonSerializer.Deserialize<GlobalIndexView>(json, ProjectionViewJson.Read)?.TotalTags == 42);

        await runner.StopAsync(CancellationToken.None);

        Assert.NotNull(viewJson);
        var view = JsonSerializer.Deserialize<GlobalIndexView>(viewJson!, ProjectionViewJson.Read);
        Assert.Equal(42, view!.TotalTags);
    }

    /// <summary>
    /// Same regression as multi-stream: DCB uses a filtered <c>ReadByQueryStreamAsync</c> read;
    /// the runner must not HiLo-jump past windows that had no query matches (advance one batch only).
    /// Prefix noise uses <see cref="OrderCreatedLocal"/> (not in the Counter-only DCB query) so empty
    /// read windows reflect filtering, not a global sequence gap.
    /// </summary>
    [Fact]
    public async Task Dcb_ColdReplay_WithSparseMatchingEvents_DoesNotSkipTailCounters()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        const int noiseBatches = 25;
        const int batchSize    = 100;
        var noiseStream = "noise:OrderAggregate:dcb-noise";
        for (var i = 0; i < noiseBatches; i++)
        {
            IEvent[] batch = new IEvent[batchSize];
            for (var j = 0; j < batchSize; j++)
                batch[j] = new OrderCreatedLocal(Guid.NewGuid(), DateTime.UtcNow, Guid.NewGuid(), "noise");

            await store.AppendAsync(noiseStream, batch, metadata: MakeMeta(), cancellationToken: cts.Token);
        }

        await store.AppendAsync(
            "any:Counter:tail",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 42)],
            metadata: MakeMeta(),
            cancellationToken: cts.Token);

        var runner = await StartRunnerAsync(
            MakeDcbRegistration(), store, views, checkpoints, cts.Token);

        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "CounterDcbFeed:v1",
            "global",
            TimeSpan.FromSeconds(60),
            json => JsonSerializer.Deserialize<GlobalIndexView>(json, ProjectionViewJson.Read)?.TotalTags == 42);

        await runner.StopAsync(CancellationToken.None);

        Assert.NotNull(viewJson);

        var view = JsonSerializer.Deserialize<GlobalIndexView>(viewJson!, ProjectionViewJson.Read);
        Assert.NotNull(view);
        Assert.Equal(42, view!.TotalTags);
    }
}
