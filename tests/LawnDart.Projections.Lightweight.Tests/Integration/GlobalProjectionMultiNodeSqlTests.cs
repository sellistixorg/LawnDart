using System.Net;
using System.Text.Json;
using LawnDart.EventStore;
using LawnDart.EventSourcing.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Matrix E SQL — Global-style per-node view isolation + GET against SqlViewStore.
/// </summary>
[Collection("SqlFlushMatrixA")]
[Trait("Category", "Integration")]
public sealed class GlobalProjectionMultiNodeSqlTests
{
    private readonly MsSqlMatrixAFixture _fx;

    public GlobalProjectionMultiNodeSqlTests(MsSqlMatrixAFixture fx) => _fx = fx;

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static LightweightProjectionOptions NodeOptions(int node, int total) => new()
    {
        PollInterval = TimeSpan.FromMilliseconds(20),
        CheckpointInterval = 1,
        BatchSize = 50,
        NodeInstance = node,
        TotalInstances = total,
        SkipTailFlushWhileCatchingUp = false
    };

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

    // ── E1 / E2 ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task E1_E2_TwoNodes_WriteDistinctSqlRows_NoCrossClobber()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_e12", cts.Token);
        var store = new InMemoryEventStore();
        var reg = MatrixBceSqlHost.MakeGlobalReg();

        await store.AppendAsync("system:Tags:1", [
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "alpha"),
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "beta")
        ], metadata: Meta(), cancellationToken: cts.Token);

        var runner0 = await StartNodeAsync(reg, store, views, checkpoints, 0, 2, cts.Token);
        var runner1 = await StartNodeAsync(reg, store, views, checkpoints, 1, 2, cts.Token);

        var id0 = ProjectionInstanceIds.ForUnpartitioned(0, 2);
        var id1 = ProjectionInstanceIds.ForUnpartitioned(1, 2);

        var json0 = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views, MatrixBceSqlHost.GlobalStorageKey, id0, TimeSpan.FromSeconds(20),
            j => JsonSerializer.Deserialize<GlobalIndexView>(j, ProjectionViewJson.Read)?.TotalTags == 2);
        var json1 = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views, MatrixBceSqlHost.GlobalStorageKey, id1, TimeSpan.FromSeconds(20),
            j => JsonSerializer.Deserialize<GlobalIndexView>(j, ProjectionViewJson.Read)?.TotalTags == 2);

        Assert.NotNull(json0);
        Assert.NotNull(json1);
        Assert.Null(await views.GetViewAsync(MatrixBceSqlHost.GlobalStorageKey, "global", cts.Token));

        var all = (await views.GetViewsByTypeAsync(MatrixBceSqlHost.GlobalStorageKey, cts.Token)).ToList();
        Assert.Equal(2, all.Count);
        Assert.Contains(all, v => v.InstanceId == id0);
        Assert.Contains(all, v => v.InstanceId == id1);

        var ck0 = await checkpoints.GetCheckpointAsync(MatrixBceSqlHost.GlobalStorageKey, 0, cts.Token);
        var ck1 = await checkpoints.GetCheckpointAsync(MatrixBceSqlHost.GlobalStorageKey, 1, cts.Token);
        Assert.NotNull(ck0);
        Assert.NotNull(ck1);

        await runner0.StopAsync(cts.Token);
        await runner1.StopAsync(cts.Token);
        runner0.Dispose();
        runner1.Dispose();
    }

    // ── E3 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task E3_GetOnEachNode_ResolvesThatNodesReplica()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_e3", cts.Token);
        var store = new InMemoryEventStore();
        var reg = MatrixBceSqlHost.MakeGlobalReg();

        await store.AppendAsync("system:Tags:1", [
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "n0"),
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "n1")
        ], metadata: Meta(), cancellationToken: cts.Token);

        var runner0 = await StartNodeAsync(reg, store, views, checkpoints, 0, 2, cts.Token);
        var runner1 = await StartNodeAsync(reg, store, views, checkpoints, 1, 2, cts.Token);

        var id0 = ProjectionInstanceIds.ForUnpartitioned(0, 2);
        var id1 = ProjectionInstanceIds.ForUnpartitioned(1, 2);
        await ProjectionRunnerTestHarness.WaitForViewAsync(
            views, MatrixBceSqlHost.GlobalStorageKey, id0, TimeSpan.FromSeconds(20),
            j => JsonSerializer.Deserialize<GlobalIndexView>(j, ProjectionViewJson.Read)?.TotalTags == 2);
        await ProjectionRunnerTestHarness.WaitForViewAsync(
            views, MatrixBceSqlHost.GlobalStorageKey, id1, TimeSpan.FromSeconds(20),
            j => JsonSerializer.Deserialize<GlobalIndexView>(j, ProjectionViewJson.Read)?.TotalTags == 2);

        await runner0.StopAsync(cts.Token);
        await runner1.StopAsync(cts.Token);
        runner0.Dispose();
        runner1.Dispose();

        // GET on node-0 host resolves global:n0; node-1 host resolves global:n1.
        var (app0, client0) = await MatrixBceSqlHost.StartQueryHostAsync(
            views, [reg], partitioning: new ConsistentHashPartitioningService(0, 2));
        await using (app0)
        {
            var r0 = await MatrixBceSqlHost.GetAsync(client0, "/api/views/tags", tenantId: null);
            Assert.Equal(HttpStatusCode.OK, r0.StatusCode);
            var body0 = await r0.Content.ReadAsStringAsync(cts.Token);
            Assert.Equal(2, JsonSerializer.Deserialize<GlobalIndexView>(body0,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!.TotalTags);

            // Confirm SQL row identity by deleting the other node's row and re-reading.
            await views.DeleteViewAsync(MatrixBceSqlHost.GlobalStorageKey, id1, cts.Token);
            var stillOk = await MatrixBceSqlHost.GetAsync(client0, "/api/views/tags", tenantId: null);
            Assert.Equal(HttpStatusCode.OK, stillOk.StatusCode);
        }

        var (app1, client1) = await MatrixBceSqlHost.StartQueryHostAsync(
            views, [reg], partitioning: new ConsistentHashPartitioningService(1, 2));
        await using (app1)
        {
            // id1 was deleted above — node-1 GET must 404 (does not fall back to n0).
            var r1 = await MatrixBceSqlHost.GetAsync(client1, "/api/views/tags", tenantId: null);
            Assert.Equal(HttpStatusCode.NotFound, r1.StatusCode);
        }
    }

    // ── E4 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task E4_SingleStream_NonOwner_DoesNotApply_SqlHasOwnerRow()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_e4", cts.Token);
        var store = new InMemoryEventStore();
        var reg = MatrixBceSqlHost.MakeCounterReg();

        string? streamId = null;
        for (var i = 0; i < 200; i++)
        {
            var candidate = $"tenant1:Counter:e4-{i}";
            var p0 = new ConsistentHashPartitioningService(0, 2);
            var p1 = new ConsistentHashPartitioningService(1, 2);
            if (p0.OwnsStream(candidate) && !p1.OwnsStream(candidate))
            {
                streamId = candidate;
                break;
            }
        }

        Assert.NotNull(streamId);
        var entityId = streamId!["tenant1:Counter:".Length..];

        await store.AppendAsync(streamId!, [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 7)
        ], metadata: Meta(), cancellationToken: cts.Token);

        var owner = await StartNodeAsync(reg, store, views, checkpoints, 0, 2, cts.Token);
        var nonOwner = await StartNodeAsync(reg, store, views, checkpoints, 1, 2, cts.Token);

        var json = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views, MatrixBceSqlHost.CounterStorageKey, streamId!, TimeSpan.FromSeconds(20),
            j => JsonSerializer.Deserialize<CounterView>(j, ProjectionViewJson.Read)?.Count == 7);
        Assert.NotNull(json);

        var rows = (await views.GetViewsByTypeAsync(MatrixBceSqlHost.CounterStorageKey, cts.Token))
            .Where(v => v.InstanceId == streamId)
            .ToList();
        Assert.Single(rows);

        // Non-owner GET serves the owner's flushed SQL row (Q5a).
        var (app, client) = await MatrixBceSqlHost.StartQueryHostAsync(
            views, [reg], partitioning: new ConsistentHashPartitioningService(1, 2));
        await using (app)
        {
            var response = await MatrixBceSqlHost.GetAsync(client, $"/api/views/counters/{entityId}");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var body = await response.Content.ReadAsStringAsync(cts.Token);
            Assert.Equal(7, JsonSerializer.Deserialize<CounterView>(body,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!.Count);
        }

        await owner.StopAsync(cts.Token);
        await nonOwner.StopAsync(cts.Token);
        owner.Dispose();
        nonOwner.Dispose();
    }

    // ── E5 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task E5_TotalInstances1_StillUsesGlobalInstanceId()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var (views, checkpoints) = await MatrixBceSqlHost.CreateSqlStoresAsync(_fx, "matrix_e5", cts.Token);
        var store = new InMemoryEventStore();
        var reg = MatrixBceSqlHost.MakeGlobalReg();

        await store.AppendAsync("system:Tags:1", [
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "solo")
        ], metadata: Meta(), cancellationToken: cts.Token);

        var runner = await StartNodeAsync(reg, store, views, checkpoints, 0, 1, cts.Token);

        var json = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views, MatrixBceSqlHost.GlobalStorageKey, "global", TimeSpan.FromSeconds(20),
            j => JsonSerializer.Deserialize<GlobalIndexView>(j, ProjectionViewJson.Read)?.TotalTags == 1);

        Assert.NotNull(json);
        Assert.Null(await views.GetViewAsync(MatrixBceSqlHost.GlobalStorageKey, "global:n0", cts.Token));

        await runner.StopAsync(cts.Token);
        runner.Dispose();
    }
}
