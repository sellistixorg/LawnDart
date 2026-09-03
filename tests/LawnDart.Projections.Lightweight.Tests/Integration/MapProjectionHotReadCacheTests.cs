using System.Net;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using LawnDart.Authorization;
using LawnDart.EventSourcing.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Admin;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Hot read cache: memory → durable, no false 404, promote, rebuild purge.
/// </summary>
[Trait("Category", "Integration")]
public sealed class MapProjectionHotReadCacheTests
{
    private const string TestScheme = "FakeScheme";
    private const string StorageKey = "CounterSummary:v1";

    private static ProjectionRegistration MakeCounterReg() =>
        new(
            handlerType: typeof(CounterSummaryProjection),
            viewType: typeof(CounterView),
            projectionName: "CounterSummary",
            kind: ProjectionKind.SingleStream,
            tenantScope: TenantScope.TenantScoped,
            streamType: "Counter",
            endpoint: new ProjectionEndpointAttribute(
                route: "/api/views/counters/{counterId}",
                requiredPermission: "Counter.View"));

    private static async Task<(WebApplication App, HttpClient Client)> StartHostAsync(
        IViewStore views,
        IProjectionReadCache cache,
        IProjectionRunnerManager? manager = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        builder.Services.AddHttpContextAccessor();
        builder.Services.AddSingleton(views);
        builder.Services.AddSingleton(cache);
        if (manager is not null)
            builder.Services.AddSingleton(manager);

        builder.Services.AddSingleton<IPartitioningService, SingleNodePartitioningService>();
        builder.Services.AddSingleton<IAuthorizationProvider, DefaultAuthorizationProvider>();
        builder.Services.AddSingleton<AuthorizationService>();
        builder.Services
            .AddAuthentication(TestScheme)
            .AddScheme<AuthenticationSchemeOptions, FakeAuthHandler>(TestScheme, _ => { });
        builder.Services.AddAuthorization();
        builder.Services.AddSingleton<IReadOnlyList<ProjectionRegistration>>([MakeCounterReg()]);

        var app = builder.Build();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapProjectionQueries();
        await app.StartAsync();
        return (app, app.GetTestServer().CreateClient());
    }

    private static async Task<HttpResponseMessage> GetAsync(HttpClient client, string path, string tenantId)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, path);
        var claimsJson = JsonSerializer.Serialize(new[]
        {
            new { type = "tenant_id", value = tenantId },
            new { type = ClaimTypes.Role, value = "Admin" }
        });
        request.Headers.Authorization = new AuthenticationHeaderValue(
            "Bearer", Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(claimsJson)));
        return await client.SendAsync(request);
    }

    [Fact]
    public async Task CacheMiss_WithDurableRow_Returns200_Not404()
    {
        var views = new InMemoryViewStore();
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);
        await views.SaveViewAsync(
            StorageKey, "tenant1:Counter:sql-only",
            JsonSerializer.Serialize(new CounterView { Count = 11 }),
            checkpoint: 7);

        var (app, client) = await StartHostAsync(views, cache);
        await using (app)
        {
            var response = await GetAsync(client, "/api/views/counters/sql-only", "tenant1");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode); // never 404 on durable hit
            Assert.Equal("7", response.Headers.GetValues(ProjectionFreshness.SequenceHeader).Single());

            var body = await response.Content.ReadAsStringAsync();
            var view = JsonSerializer.Deserialize<CounterView>(body,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            Assert.Equal(11, view!.Count);
        }
    }

    [Fact]
    public async Task HotCacheHit_ReturnsMemorySource_EvenWhenDurableLags()
    {
        var views = new InMemoryViewStore();
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);

        // Durable is stale / empty; cache has current apply snapshot.
        cache.Set(StorageKey, "tenant1:Counter:hot",
            JsonSerializer.Serialize(new CounterView { Count = 99 }), 50);

        var (app, client) = await StartHostAsync(views, cache);
        await using (app)
        {
            var response = await GetAsync(client, "/api/views/counters/hot", "tenant1");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(
                ProjectionFreshness.ReadSourceMemory,
                response.Headers.GetValues(ProjectionFreshness.ReadSourceHeader).Single());
            Assert.Equal("50", response.Headers.GetValues(ProjectionFreshness.SequenceHeader).Single());

            var body = await response.Content.ReadAsStringAsync();
            var view = JsonSerializer.Deserialize<CounterView>(body,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            Assert.Equal(99, view!.Count);
        }
    }

    [Fact]
    public async Task DurableGet_PromotesIntoHotCache()
    {
        var views = new InMemoryViewStore();
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot, promoteAfterHits: 1);
        await views.SaveViewAsync(
            StorageKey, "tenant1:Counter:promo",
            JsonSerializer.Serialize(new CounterView { Count = 4 }),
            checkpoint: 12);

        var (app, client) = await StartHostAsync(views, cache);
        await using (app)
        {
            var first = await GetAsync(client, "/api/views/counters/promo", "tenant1");
            Assert.Equal(HttpStatusCode.OK, first.StatusCode);
            Assert.True(cache.TryGet(StorageKey, "tenant1:Counter:promo", out var cached));
            Assert.Equal(12, cached.Sequence);

            // Clear durable — promoted cache must still serve
            await views.DeleteViewAsync(StorageKey, "tenant1:Counter:promo");
            var second = await GetAsync(client, "/api/views/counters/promo", "tenant1");
            Assert.Equal(HttpStatusCode.OK, second.StatusCode);
            Assert.Equal(
                ProjectionFreshness.ReadSourceMemory,
                second.Headers.GetValues(ProjectionFreshness.ReadSourceHeader).Single());
        }
    }

    [Fact]
    public async Task RunnerApply_PublishesToCache_GetServesMemoryAheadOfDurable()
    {
        var durable = new InMemoryViewStore();
        // Flush always fails so durable stays empty while apply still updates the hot cache.
        var failingViews = new FailNSaveViewsStore(durable, failCount: 10_000);
        var checkpoints = new InMemoryCheckpointStore(durable);
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(15),
            CheckpointInterval = 1,
            BatchSize = 50,
            SkipTailFlushWhileCatchingUp = false,
            ReadCacheMode = ProjectionReadCacheMode.Hot
        };

        var reg = MakeCounterReg();
        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => new LightweightProjectionRunnerService(
                r, store, failingViews, checkpoints, new SingleNodePartitioningService(), options,
                readCache: cache));

        await store.AppendAsync(
            "tenant1:Counter:live",
            [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 5)],
            metadata: new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "test" },
            cancellationToken: cts.Token);

        await manager.StartAsync(StorageKey, CancellationToken.None);

        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline && !cache.TryGet(StorageKey, "tenant1:Counter:live", out _))
            await Task.Delay(25, CancellationToken.None);

        Assert.True(cache.TryGet(StorageKey, "tenant1:Counter:live", out _), "Expected apply to publish to read cache");
        Assert.Null(await durable.GetViewAsync(StorageKey, "tenant1:Counter:live", cts.Token));

        var (app, client) = await StartHostAsync(durable, cache, manager);
        await using (app)
        {
            var response = await GetAsync(client, "/api/views/counters/live", "tenant1");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(
                ProjectionFreshness.ReadSourceMemory,
                response.Headers.GetValues(ProjectionFreshness.ReadSourceHeader).Single());

            var body = await response.Content.ReadAsStringAsync();
            var view = JsonSerializer.Deserialize<CounterView>(body,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            Assert.Equal(5, view!.Count);
        }

        await manager.StopAllAsync(cts.Token);
    }

    [Fact]
    public async Task Rebuild_ClearsHotCache_SoPreRebuildBodyIsNotServed()
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(15),
            CheckpointInterval = 1,
            BatchSize = 50,
            SkipTailFlushWhileCatchingUp = false
        };

        var reg = MakeCounterReg();
        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => new LightweightProjectionRunnerService(
                r, store, views, checkpoints, new SingleNodePartitioningService(), options,
                readCache: cache));

        // Seed a pre-rebuild cache entry that must disappear after RebuildAsync.
        cache.Set(StorageKey, "tenant1:Counter:gone",
            JsonSerializer.Serialize(new CounterView { Count = 123 }), 9);

        var catalog = new ProjectionRegistrationCatalog([reg]);
        var admin = new BoundedContextProjectionAdmin(
            manager, checkpoints, views, catalog, new SingleNodePartitioningService(),
            readCache: cache);

        await admin.RebuildAsync("CounterSummary", ct: cts.Token);

        Assert.False(cache.TryGet(StorageKey, "tenant1:Counter:gone", out _),
            "Rebuild must clear accelerators for the storage key");

        var (app, client) = await StartHostAsync(views, cache, manager);
        await using (app)
        {
            var response = await GetAsync(client, "/api/views/counters/gone", "tenant1");
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        }

        await manager.StopAllAsync(cts.Token);
    }
}
