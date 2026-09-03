using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
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
/// Integration tests for <see cref="LightweightProjectionRunnerService"/> using an
/// in-process event store, in-memory view store, and in-memory checkpoint store.
/// </summary>
[Trait("Category", "Integration")]
public class ProjectionRunnerServiceTests
{
    // ── Helpers ────────────────────────────────────────────────────────────────

    private static readonly LightweightProjectionOptions DefaultOptions = new()
    {
        PollInterval = TimeSpan.FromMilliseconds(50),
        CheckpointInterval = 10,
        BatchSize = 100
    };

    private static ProjectionRegistration MakeSingleStreamRegistration() =>
        new(
            handlerType: typeof(CounterSummaryProjection),
            viewType: typeof(CounterView),
            projectionName: "CounterSummary",
            kind: ProjectionKind.SingleStream,
            tenantScope: TenantScope.TenantScoped,
            streamType: "Counter",
            endpoint: new ProjectionEndpointAttribute("/api/views/counters/{id}", "Counter.View"));

    private static ProjectionRegistration MakeGlobalRegistration() =>
        new(
            handlerType: typeof(GlobalTagIndexProjection),
            viewType: typeof(GlobalIndexView),
            projectionName: "GlobalTagIndex",
            kind: ProjectionKind.Global,
            tenantScope: TenantScope.SystemGlobal,
            endpoint: new ProjectionEndpointAttribute("/api/views/tags"));

    private static (IEventStore store, IViewStore views, ICheckpointStore checkpoints)
        BuildInMemoryStores()
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var eventStore = new LawnDart.EventSourcing.EventStore.InMemoryEventStore();
        return (eventStore, views, checkpoints);
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

    // ── Tests ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ColdStart_ProcessesEvents_And_WritesView()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Append two events to a counter stream
        var streamId = "tenant1:Counter:aaa";
        await store.AppendAsync(streamId, [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 5),
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 3)
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var runner = await StartRunnerAsync(
            MakeSingleStreamRegistration(), store, views, checkpoints, cts.Token);

        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "CounterSummary:v1",
            streamId,
            TimeSpan.FromSeconds(60),
            json => JsonSerializer.Deserialize<CounterView>(json, ProjectionViewJson.Read)?.Count == 8);

        await runner.StopAsync(CancellationToken.None);

        Assert.NotNull(viewJson);

        var view = JsonSerializer.Deserialize<CounterView>(viewJson!, ProjectionViewJson.Read);
        Assert.NotNull(view);
        Assert.Equal(8, view!.Count);
    }

    [Fact]
    public async Task ColdStart_GlobalProjection_ProcessesEvents()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await store.AppendAsync("system:Tags:1", [
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "alpha"),
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "beta")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var runner = await StartRunnerAsync(
            MakeGlobalRegistration(), store, views, checkpoints, cts.Token);

        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "GlobalTagIndex:v1",
            "global",
            TimeSpan.FromSeconds(60),
            json => JsonSerializer.Deserialize<GlobalIndexView>(json, ProjectionViewJson.Read)?.TotalTags == 2);

        await runner.StopAsync(CancellationToken.None);

        Assert.NotNull(viewJson);

        var view = JsonSerializer.Deserialize<GlobalIndexView>(viewJson!, ProjectionViewJson.Read);
        Assert.NotNull(view);
        Assert.Equal(2, view!.TotalTags);
    }

    [Fact]
    public async Task NewStreamDiscovered_CreatesNewInstance()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var runner = await StartRunnerAsync(
            MakeSingleStreamRegistration(), store, views, checkpoints, cts.Token);

        // Append event AFTER runner started → new instance should be created on the fly
        await Task.Delay(100, CancellationToken.None); // let runner reach steady state first

        var newStreamId = "tenant1:Counter:new-one";
        await store.AppendAsync(newStreamId, [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 42)
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "CounterSummary:v1",
            newStreamId,
            TimeSpan.FromSeconds(60),
            json => JsonSerializer.Deserialize<CounterView>(json, ProjectionViewJson.Read)?.Count == 42);

        await runner.StopAsync(CancellationToken.None);

        Assert.NotNull(viewJson);

        var view = JsonSerializer.Deserialize<CounterView>(viewJson!, ProjectionViewJson.Read);
        Assert.Equal(42, view!.Count);
    }

    [Fact]
    public async Task GracefulStop_SavesCheckpoint()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await store.AppendAsync("tenant1:Counter:bbb", [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var runner = await StartRunnerAsync(
            MakeSingleStreamRegistration(), store, views, checkpoints, cts.Token);

        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints,
            "CounterSummary:v1",
            minPosition: 1,
            TimeSpan.FromSeconds(60));

        await runner.StopAsync(CancellationToken.None);

        var savedCheckpoint = await checkpoints.GetCheckpointAsync(
            "CounterSummary:v1", nodeId: 0, CancellationToken.None);

        Assert.NotNull(savedCheckpoint);
        Assert.True(savedCheckpoint!.LastSequencePosition > 0);
    }

    [Fact]
    public async Task WarmRestart_RestoresState_AndContinues()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var reg = MakeSingleStreamRegistration();
        var streamId = "tenant1:Counter:ccc";

        // First run — process 2 events
        await store.AppendAsync(streamId, [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 10)
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var runner1 = await StartRunnerAsync(reg, store, views, checkpoints, cts.Token);
        var firstJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "CounterSummary:v1",
            streamId,
            TimeSpan.FromSeconds(60),
            json => JsonSerializer.Deserialize<CounterView>(json, ProjectionViewJson.Read)?.Count == 10);
        Assert.NotNull(firstJson);
        await runner1.StopAsync(CancellationToken.None);

        // Second run — should restore saved view (count=10) and process 1 more event
        await store.AppendAsync(streamId, [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 7)
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var runner2 = await StartRunnerAsync(reg, store, views, checkpoints, cts.Token);
        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "CounterSummary:v1",
            streamId,
            TimeSpan.FromSeconds(60),
            json => JsonSerializer.Deserialize<CounterView>(json, ProjectionViewJson.Read)?.Count == 17);
        await runner2.StopAsync(CancellationToken.None);

        Assert.NotNull(viewJson);
        var view = JsonSerializer.Deserialize<CounterView>(viewJson!, ProjectionViewJson.Read);

        Assert.Equal(17, view!.Count); // 10 + 7
    }

    [Fact]
    public async Task SingleStreamProjection_IgnoresUnrelatedStreamTypes()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Append to an unrelated stream type
        await store.AppendAsync("tenant1:Widget:xyz", [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 99)
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var runner = await StartRunnerAsync(
            MakeSingleStreamRegistration(), store, views, checkpoints, cts.Token);

        await Task.Delay(500, CancellationToken.None);
        await runner.StopAsync(CancellationToken.None);

        // No view should be created for a Widget stream in a Counter projection
        var viewJson = await views.GetViewAsync("CounterSummary:v1", "tenant1:Widget:xyz", CancellationToken.None);
        Assert.Null(viewJson);
    }
}
