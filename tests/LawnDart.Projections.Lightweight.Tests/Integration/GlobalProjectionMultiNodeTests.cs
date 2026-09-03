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
/// Matrix E — Global-style per-node view isolation.
/// </summary>
[Trait("Category", "Integration")]
public class GlobalProjectionMultiNodeTests
{
    private static ProjectionRegistration MakeGlobalRegistration() =>
        new(
            handlerType: typeof(GlobalTagIndexProjection),
            viewType: typeof(GlobalIndexView),
            projectionName: "GlobalTagIndex",
            kind: ProjectionKind.Global,
            tenantScope: TenantScope.SystemGlobal,
            endpoint: new ProjectionEndpointAttribute("/api/views/tags"));

    private static LightweightProjectionOptions NodeOptions(int node, int total) => new()
    {
        PollInterval       = TimeSpan.FromMilliseconds(20),
        CheckpointInterval = 1,
        BatchSize          = 50,
        NodeInstance       = node,
        TotalInstances     = total
    };

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static (IEventStore store, IViewStore views, ICheckpointStore checkpoints) BuildStores()
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var eventStore = new LawnDart.EventSourcing.EventStore.InMemoryEventStore();
        return (eventStore, views, checkpoints);
    }

    private static async Task<LightweightProjectionRunnerService> StartNodeAsync(
        ProjectionRegistration reg,
        IEventStore store,
        IViewStore views,
        ICheckpointStore checkpoints,
        int node,
        int total,
        CancellationToken ct)
    {
        _ = ct;
        var options = NodeOptions(node, total);
        var partitioning = new ConsistentHashPartitioningService(node, total);
        var runner = new LightweightProjectionRunnerService(
            reg, store, views, checkpoints, partitioning, options);
        await runner.StartAsync(CancellationToken.None);
        return runner;
    }

    [Fact]
    public async Task E1_E2_TwoNodes_WriteDistinctViewRows_NoCrossClobber()
    {
        var (store, views, checkpoints) = BuildStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var reg = MakeGlobalRegistration();
        var storageKey = reg.StorageKey;

        await store.AppendAsync("system:Tags:1", [
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "alpha"),
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "beta")
        ], metadata: Meta(), cancellationToken: cts.Token);

        var runner0 = await StartNodeAsync(reg, store, views, checkpoints, 0, 2, cts.Token);
        var runner1 = await StartNodeAsync(reg, store, views, checkpoints, 1, 2, cts.Token);

        var id0 = ProjectionInstanceIds.ForUnpartitioned(0, 2);
        var id1 = ProjectionInstanceIds.ForUnpartitioned(1, 2);
        Assert.Equal("global:n0", id0);
        Assert.Equal("global:n1", id1);

        var json0 = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views, storageKey, id0, TimeSpan.FromSeconds(30),
            j => JsonSerializer.Deserialize<GlobalIndexView>(j, ProjectionViewJson.Read)?.TotalTags == 2);
        var json1 = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views, storageKey, id1, TimeSpan.FromSeconds(30),
            j => JsonSerializer.Deserialize<GlobalIndexView>(j, ProjectionViewJson.Read)?.TotalTags == 2);

        Assert.NotNull(json0);
        Assert.NotNull(json1);

        // Shared "global" row must not be used when TotalInstances > 1
        var legacy = await views.GetViewAsync(storageKey, "global", cts.Token);
        Assert.Null(legacy);

        var all = (await views.GetViewsByTypeAsync(storageKey, cts.Token)).ToList();
        Assert.Equal(2, all.Count);
        Assert.Contains(all, v => v.InstanceId == id0);
        Assert.Contains(all, v => v.InstanceId == id1);

        // Independent checkpoints per node
        var ck0 = await checkpoints.GetCheckpointAsync(storageKey, 0, cts.Token);
        var ck1 = await checkpoints.GetCheckpointAsync(storageKey, 1, cts.Token);
        Assert.NotNull(ck0);
        Assert.NotNull(ck1);
        Assert.True(ck0!.LastSequencePosition >= 1);
        Assert.True(ck1!.LastSequencePosition >= 1);

        await runner0.StopAsync(cts.Token);
        await runner1.StopAsync(cts.Token);
        runner0.Dispose();
        runner1.Dispose();
    }

    [Fact]
    public async Task E5_SingleNode_StillUsesGlobalInstanceId()
    {
        var (store, views, checkpoints) = BuildStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var reg = MakeGlobalRegistration();

        await store.AppendAsync("system:Tags:1", [
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "solo")
        ], metadata: Meta(), cancellationToken: cts.Token);

        var runner = await StartNodeAsync(reg, store, views, checkpoints, 0, 1, cts.Token);

        var json = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views, reg.StorageKey, "global", TimeSpan.FromSeconds(30),
            j => JsonSerializer.Deserialize<GlobalIndexView>(j, ProjectionViewJson.Read)?.TotalTags == 1);

        Assert.NotNull(json);
        Assert.Null(await views.GetViewAsync(reg.StorageKey, "global:n0", cts.Token));

        await runner.StopAsync(cts.Token);
        runner.Dispose();
    }

    [Fact]
    public async Task E4_SingleStream_NonOwner_DoesNotWriteOwnedStream()
    {
        var (store, views, checkpoints) = BuildStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var reg = new ProjectionRegistration(
            handlerType: typeof(CounterSummaryProjection),
            viewType: typeof(CounterView),
            projectionName: "CounterSummary",
            kind: ProjectionKind.SingleStream,
            tenantScope: TenantScope.TenantScoped,
            streamType: "Counter",
            endpoint: new ProjectionEndpointAttribute("/api/views/counters/{id}", "Counter.View"));

        // Find a stream owned by node 0 but not node 1
        string? streamId = null;
        for (var i = 0; i < 200; i++)
        {
            var candidate = $"tenant1:Counter:stream-{i}";
            var p0 = new ConsistentHashPartitioningService(0, 2);
            var p1 = new ConsistentHashPartitioningService(1, 2);
            if (p0.OwnsStream(candidate) && !p1.OwnsStream(candidate))
            {
                streamId = candidate;
                break;
            }
        }

        Assert.NotNull(streamId);

        await store.AppendAsync(streamId!, [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 7)
        ], metadata: Meta(), cancellationToken: cts.Token);

        var owner = await StartNodeAsync(reg, store, views, checkpoints, 0, 2, cts.Token);
        var nonOwner = await StartNodeAsync(reg, store, views, checkpoints, 1, 2, cts.Token);

        var json = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views, reg.StorageKey, streamId!, TimeSpan.FromSeconds(30),
            j => JsonSerializer.Deserialize<CounterView>(j, ProjectionViewJson.Read)?.Count == 7);

        Assert.NotNull(json);

        // Non-owner must not hold a conflicting write — only one view row for this stream
        var rows = (await views.GetViewsByTypeAsync(reg.StorageKey, cts.Token))
            .Where(v => v.InstanceId == streamId)
            .ToList();
        Assert.Single(rows);

        await owner.StopAsync(cts.Token);
        await nonOwner.StopAsync(cts.Token);
        owner.Dispose();
        nonOwner.Dispose();
    }

    [Fact]
    public async Task MultiStream_NonOwner_DoesNotWriteEntityInstance()
    {
        var (store, views, checkpoints) = BuildStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var reg = new ProjectionRegistration(
            handlerType: typeof(OrderFulfillmentSummaryProjection),
            viewType: typeof(FulfillmentView),
            projectionName: "OrderFulfillmentSummary",
            kind: ProjectionKind.MultiStream,
            tenantScope: TenantScope.TenantGlobal,
            dcbQueryTypes: [
                EventTypeNameResolver.GetName(typeof(OrderCreatedLocal)),
                EventTypeNameResolver.GetName(typeof(ShipmentDispatchedLocal))],
            endpoint: new ProjectionEndpointAttribute("/api/views/fulfillment/{orderId}"));

        // Find an entity id owned by node 0 only
        Guid? orderId = null;
        for (var i = 0; i < 500; i++)
        {
            var id = Guid.NewGuid();
            var entityKey = id.ToString();
            var p0 = new ConsistentHashPartitioningService(0, 2);
            var p1 = new ConsistentHashPartitioningService(1, 2);
            if (p0.OwnsStream(entityKey) && !p1.OwnsStream(entityKey))
            {
                orderId = id;
                break;
            }
        }

        Assert.NotNull(orderId);
        var entityKeyOwned = orderId!.Value.ToString();

        await store.AppendAsync($"OrderAggregate:{orderId}", [
            new OrderCreatedLocal(Guid.NewGuid(), DateTime.UtcNow, orderId.Value, "Acme")
        ], metadata: Meta(), cancellationToken: cts.Token);

        var owner = await StartNodeAsync(reg, store, views, checkpoints, 0, 2, cts.Token);
        var nonOwner = await StartNodeAsync(reg, store, views, checkpoints, 1, 2, cts.Token);

        var json = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views, reg.StorageKey, entityKeyOwned, TimeSpan.FromSeconds(30),
            j => JsonSerializer.Deserialize<FulfillmentView>(j, ProjectionViewJson.Read)?.Customer == "Acme");

        Assert.NotNull(json);

        var rows = (await views.GetViewsByTypeAsync(reg.StorageKey, cts.Token))
            .Where(v => v.InstanceId == entityKeyOwned)
            .ToList();
        Assert.Single(rows);

        await owner.StopAsync(cts.Token);
        await nonOwner.StopAsync(cts.Token);
        owner.Dispose();
        nonOwner.Dispose();
    }
}
