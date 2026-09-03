using System.Net;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using LawnDart.Authorization;
using LawnDart.EventStore;
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
/// Matrix A SQL smoke against Testcontainers SQL Server.
/// A9 load is intentionally omitted (on-demand).
/// </summary>
[Collection("SqlFlushMatrixA")]
[Trait("Category", "Integration")]
public sealed class FlushCheckpointMatrixASqlTests
{
    private const string StorageKey = "CounterSummary:v1";

    private readonly MsSqlMatrixAFixture _fx;

    public FlushCheckpointMatrixASqlTests(MsSqlMatrixAFixture fx) => _fx = fx;

    private static ProjectionRegistration MakeCounterReg() =>
        new(
            handlerType: typeof(CounterSummaryProjection),
            viewType: typeof(CounterView),
            projectionName: "CounterSummary",
            kind: ProjectionKind.SingleStream,
            tenantScope: TenantScope.TenantScoped,
            streamType: "Counter",
            endpoint: new ProjectionEndpointAttribute("/api/views/counters/{counterId}", "Counter.View"));

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static LightweightProjectionOptions AggressiveOptions => new()
    {
        PollInterval = TimeSpan.FromMilliseconds(15),
        CheckpointInterval = 1,
        BatchSize = 50,
        SkipTailFlushWhileCatchingUp = false
    };

    private async Task<(SqlViewStore Views, SqlCheckpointStore Checkpoints)> CreateSqlStoresAsync(
        string schema, CancellationToken ct)
    {
        var views = new SqlViewStore(_fx.ConnectionString, schemaName: schema);
        var checkpoints = new SqlCheckpointStore(_fx.ConnectionString, views, schemaName: schema);
        await views.InitializeSchemaAsync(ct);
        await checkpoints.InitializeSchemaAsync(ct);
        return (views, checkpoints);
    }

    private static async Task AppendCountersAsync(IEventStore store, int count, string prefix, CancellationToken ct)
    {
        for (var i = 0; i < count; i++)
        {
            await store.AppendAsync(
                $"tenant1:Counter:{prefix}-{i}",
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, i + 1)],
                metadata: Meta(),
                cancellationToken: ct);
        }
    }

    private static async Task<Dictionary<string, string>> SnapshotViewsAsync(
        IViewStore views, CancellationToken ct)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (id, json) in await views.GetViewsByTypeAsync(StorageKey, ct))
            map[id] = json;
        return map;
    }

    // ── A1 ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A1_HappyFlush_ViewsAndCheckpointInSql_GetReturns200()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var (views, checkpoints) = await CreateSqlStoresAsync("matrix_a1", cts.Token);
        var store = new InMemoryEventStore();
        const int n = 5;
        await AppendCountersAsync(store, n, "a1", cts.Token);

        var head = await store.GetCurrentSequenceAsync(cts.Token);
        var runner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), AggressiveOptions);
        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, StorageKey, head, TimeSpan.FromSeconds(30), cts.Token);
        await runner.StopAsync(cts.Token);
        runner.Dispose();

        var ckpt = await checkpoints.GetCheckpointAsync(StorageKey, 0, cts.Token);
        Assert.NotNull(ckpt);
        Assert.Equal(head, ckpt!.LastSequencePosition);

        var all = (await views.GetViewsByTypeAsync(StorageKey, cts.Token)).ToList();
        Assert.Equal(n, all.Count);

        // GET via MapProjectionQueries against the same SQL view store
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddHttpContextAccessor();
        builder.Services.AddSingleton<IViewStore>(views);
        builder.Services.AddSingleton<IPartitioningService, SingleNodePartitioningService>();
        builder.Services.AddSingleton<IAuthorizationProvider, DefaultAuthorizationProvider>();
        builder.Services.AddSingleton<AuthorizationService>();
        builder.Services
            .AddAuthentication("FakeScheme")
            .AddScheme<AuthenticationSchemeOptions, FakeAuthHandler>("FakeScheme", _ => { });
        builder.Services.AddAuthorization();
        builder.Services.AddSingleton<IReadOnlyList<ProjectionRegistration>>([MakeCounterReg()]);

        await using var app = builder.Build();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapProjectionQueries();
        await app.StartAsync(CancellationToken.None);

        using var client = app.GetTestServer().CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Get, "/api/views/counters/a1-0");
        var claimsJson = JsonSerializer.Serialize(new[]
        {
            new { type = "tenant_id", value = "tenant1" },
            new { type = ClaimTypes.Role, value = "Admin" }
        });
        request.Headers.Authorization = new AuthenticationHeaderValue(
            "Bearer", Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(claimsJson)));

        var response = await client.SendAsync(request, cts.Token);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync(cts.Token);
        var view = JsonSerializer.Deserialize<CounterView>(body,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        Assert.Equal(1, view!.Count);
    }

    // ── A2 / A3 ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task A2_A3_BulkSameAsLoop_IdenticalEndState()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var store = new InMemoryEventStore();
        const int n = 8;
        await AppendCountersAsync(store, n, "a23", cts.Token);

        var head = await store.GetCurrentSequenceAsync(cts.Token);

        // Bulk (native SqlViewStore.SaveViewsAsync)
        var (bulkViews, bulkCkpt) = await CreateSqlStoresAsync("matrix_a2_bulk", cts.Token);
        var bulkRunner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, bulkViews, bulkCkpt,
            new SingleNodePartitioningService(), AggressiveOptions);
        await bulkRunner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            bulkCkpt, StorageKey, head, TimeSpan.FromSeconds(30), cts.Token);
        await bulkRunner.StopAsync(cts.Token);
        bulkRunner.Dispose();

        // Loop fallback forced
        var (loopInner, loopCkpt) = await CreateSqlStoresAsync("matrix_a2_loop", cts.Token);
        var loopViews = new LoopOnlyViewStore(loopInner);
        var loopRunner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, loopViews, loopCkpt,
            new SingleNodePartitioningService(), AggressiveOptions);
        await loopRunner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            loopCkpt, StorageKey, head, TimeSpan.FromSeconds(30), cts.Token);
        await loopRunner.StopAsync(cts.Token);
        loopRunner.Dispose();

        Assert.True(loopViews.SaveViewsCalls >= 1);
        Assert.True(loopViews.SaveViewCalls >= n);

        var bulkSnap = await SnapshotViewsAsync(bulkViews, cts.Token);
        var loopSnap = await SnapshotViewsAsync(loopInner, cts.Token);
        Assert.Equal(bulkSnap.Count, loopSnap.Count);
        foreach (var (id, bulkJson) in bulkSnap)
        {
            Assert.True(loopSnap.TryGetValue(id, out var loopJson));
            var bulkCount = JsonSerializer.Deserialize<CounterView>(bulkJson, ProjectionViewJson.Read)!.Count;
            var loopCount = JsonSerializer.Deserialize<CounterView>(loopJson!, ProjectionViewJson.Read)!.Count;
            Assert.Equal(bulkCount, loopCount);
        }

        var bCkpt = await bulkCkpt.GetCheckpointAsync(StorageKey, 0, cts.Token);
        var lCkpt = await loopCkpt.GetCheckpointAsync(StorageKey, 0, cts.Token);
        Assert.Equal(bCkpt!.LastSequencePosition, lCkpt!.LastSequencePosition);
    }

    [Fact]
    public async Task A2_DopOptions_DoNotChangeSqlEndState()
    {
        // FlushMaxDegreeOfParallelism is unused on the bulk SaveViewsAsync path; still assert
        // option differences do not change durable SQL outcomes (regression guard).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var store = new InMemoryEventStore();
        const int n = 6;
        await AppendCountersAsync(store, n, "adop", cts.Token);

        var head = await store.GetCurrentSequenceAsync(cts.Token);

        async Task<(Dictionary<string, int> Counts, long Ckpt)> RunWithDopAsync(int dop, string schema)
        {
            var (views, checkpoints) = await CreateSqlStoresAsync(schema, cts.Token);
            var options = new LightweightProjectionOptions
            {
                PollInterval = TimeSpan.FromMilliseconds(15),
                CheckpointInterval = 1,
                BatchSize = 50,
                FlushMaxDegreeOfParallelism = dop,
                SkipTailFlushWhileCatchingUp = false
            };
            var runner = new LightweightProjectionRunnerService(
                MakeCounterReg(), store, views, checkpoints,
                new SingleNodePartitioningService(), options);
            await runner.StartAsync(CancellationToken.None);
            await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
                checkpoints, StorageKey, head, TimeSpan.FromSeconds(30), cts.Token);
            await runner.StopAsync(cts.Token);
            runner.Dispose();

            var ckpt = await checkpoints.GetCheckpointAsync(StorageKey, 0, cts.Token);
            var counts = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var (id, json) in await views.GetViewsByTypeAsync(StorageKey, cts.Token))
                counts[id] = JsonSerializer.Deserialize<CounterView>(json, ProjectionViewJson.Read)!.Count;
            return (counts, ckpt!.LastSequencePosition);
        }

        var (c1, p1) = await RunWithDopAsync(1, "matrix_a2_dop1");
        var (c8, p8) = await RunWithDopAsync(8, "matrix_a2_dop8");
        Assert.Equal(head, p1);
        Assert.Equal(p1, p8);
        Assert.Equal(n, c1.Count);
        Assert.Equal(c1.Count, c8.Count);
        foreach (var (id, count) in c1)
            Assert.Equal(count, c8[id]);
    }

    // ── A7 / A10 ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task A7_A10_GracefulStopAndWarmRestart_SqlMatches()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var (views, checkpoints) = await CreateSqlStoresAsync("matrix_a7", cts.Token);
        var store = new InMemoryEventStore();
        const int n = 6;
        await AppendCountersAsync(store, n, "a7", cts.Token);

        var head = await store.GetCurrentSequenceAsync(cts.Token);
        var runner1 = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), AggressiveOptions);
        await runner1.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, StorageKey, head, TimeSpan.FromSeconds(30), cts.Token);
        await runner1.StopAsync(cts.Token); // A10 graceful stop final flush
        runner1.Dispose();

        var snap1 = await SnapshotViewsAsync(views, cts.Token);
        var ckpt1 = await checkpoints.GetCheckpointAsync(StorageKey, 0, cts.Token);

        var runner2 = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, views, checkpoints,
            new SingleNodePartitioningService(), AggressiveOptions);
        await runner2.StartAsync(CancellationToken.None);
        await Task.Delay(120, cts.Token);
        await runner2.StopAsync(cts.Token);
        runner2.Dispose();

        var snap2 = await SnapshotViewsAsync(views, cts.Token);
        var ckpt2 = await checkpoints.GetCheckpointAsync(StorageKey, 0, cts.Token);
        Assert.Equal(ckpt1!.LastSequencePosition, ckpt2!.LastSequencePosition);
        Assert.Equal(snap1.Count, snap2.Count);
        foreach (var (id, json) in snap1)
        {
            Assert.Equal(
                JsonSerializer.Deserialize<CounterView>(json, ProjectionViewJson.Read)!.Count,
                JsonSerializer.Deserialize<CounterView>(snap2[id], ProjectionViewJson.Read)!.Count);
        }
    }

    // ── A11 ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A11_MultiContextIsolation_FlushDoesNotCrossSchemas()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var (viewsA, ckptA) = await CreateSqlStoresAsync("ctx_a", cts.Token);
        var (viewsB, _) = await CreateSqlStoresAsync("ctx_b", cts.Token);

        var store = new InMemoryEventStore();
        await AppendCountersAsync(store, 3, "iso", cts.Token);

        var runner = new LightweightProjectionRunnerService(
            MakeCounterReg(), store, viewsA, ckptA,
            new SingleNodePartitioningService(), AggressiveOptions);
        await runner.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            ckptA, StorageKey, 2, TimeSpan.FromSeconds(30), cts.Token);
        await runner.StopAsync(cts.Token);
        runner.Dispose();

        Assert.NotEmpty(await viewsA.GetViewsByTypeAsync(StorageKey, cts.Token));
        Assert.Empty(await viewsB.GetViewsByTypeAsync(StorageKey, cts.Token));

        await using var conn = new SqlConnection(_fx.ConnectionString);
        await conn.OpenAsync(cts.Token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM [ctx_b].[ProjectionViews]";
        var countB = (int)(await cmd.ExecuteScalarAsync(cts.Token))!;
        Assert.Equal(0, countB);
    }
}
