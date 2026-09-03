using System.Net;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart.Authorization;
using LawnDart.EventSourcing;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Admin;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// In-process TestHost tests for <c>MapProjectionAdminApi()</c>.
/// </summary>
[Trait("Category", "Integration")]
public sealed class MapProjectionAdminApiTests : IAsyncLifetime
{
    private const string TestScheme = "FakeAdminScheme";
    private const string Listing = "listing";
    private const string Inbound = "inbound";
    private const string CounterStorageKey = "CounterSummary:v1";

    private static readonly IReadOnlyList<ProjectionRegistration> TestRegistrations =
        ProjectionScanner.Scan([typeof(CounterSummaryProjection).Assembly])
            .Where(r => r.LogicalName is "CounterSummary" or "VersionedCounterSummary")
            .ToList();

    private WebApplication? _app;
    private HttpClient? _client;

    private TestServer Server => _app!.GetTestServer();
    private HttpClient Client => _client!;

    public async Task InitializeAsync()
    {
        var builder = CreateBaseBuilder();
        RegisterContexts(builder.Services, [Listing, Inbound], multiNode: false);

        builder.Services
            .AddAuthentication(TestScheme)
            .AddScheme<AuthenticationSchemeOptions, FakeDebugAuthHandler>(TestScheme, _ => { });
        builder.Services.AddAuthorization();

        _app = builder.Build();
        _app.UseAuthentication();
        _app.UseAuthorization();
        _app.MapProjectionAdminApi(opts =>
        {
            opts.RequireAuthentication = true;
        });

        await _app.StartAsync();
        _client = Server.CreateClient();
    }

    public async Task DisposeAsync()
    {
        _client?.Dispose();
        if (_app is null)
            return;

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await _app.StopAsync(cts.Token);
        }
        catch (Exception)
        {
            // Runner shutdown can race with TestServer stop timeout; fixture teardown must not fail tests.
        }

        await _app.DisposeAsync();
    }

    // ── Map / discovery ───────────────────────────────────────────────────────

    [Fact]
    public void MapProjectionAdminApi_MarksBothContextsAsMapped()
    {
        var registration = Server.Services.GetRequiredService<ProjectionAdminApiRegistration>();
        Assert.True(registration.IsRebuildMapped(Listing));
        Assert.True(registration.IsRebuildMapped(Inbound));
        Assert.Contains(Listing, registration.MappedContextNames);
        Assert.Contains(Inbound, registration.MappedContextNames);
    }

    // ── Auth ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Rebuild_Unauthenticated_Returns401()
    {
        var response = await Client.PostAsync($"/{Listing}/projections/CounterSummary/rebuild", null);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task Rebuild_AuthenticatedWithoutPolicyClaim_Returns403()
    {
        await using var host = await CreateHostAsync(
            contextNames: [Listing],
            configureAuth: services =>
            {
                services.AddAuthorization(o =>
                    o.AddPolicy("RebuildAdmin", p => p.RequireClaim("rebuild", "allow")));
            },
            configureAdmin: opts =>
            {
                opts.RequireAuthentication = true;
                opts.AuthorizationPolicyNames = ["RebuildAdmin"];
            });

        using var client = host.GetTestServer().CreateClient();
        var response = await client.SendAsync(
            AuthRequest(HttpMethod.Post, $"/{Listing}/projections/CounterSummary/rebuild"));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    // ── Unknown name ──────────────────────────────────────────────────────────

    [Fact]
    public async Task Rebuild_UnknownProjection_Returns404()
    {
        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Post, $"/{Listing}/projections/DoesNotExist/rebuild"));

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var doc = await ParseJson(response);
        Assert.Equal(404, doc.RootElement.GetProperty("status").GetInt32());
        Assert.Equal("Unknown projection", doc.RootElement.GetProperty("title").GetString());
    }

    // ── Multi-node interlock ──────────────────────────────────────────────────

    [Fact]
    public async Task Rebuild_MultiNode_Returns409_AndLeavesStoresUntouched()
    {
        await using var host = await CreateHostAsync(
            contextNames: [Listing],
            multiNode: true,
            configureAdmin: opts => opts.RequireAuthentication = true);

        var views = host.Services.GetRequiredKeyedService<IViewStore>(Listing);
        var checkpoints = host.Services.GetRequiredKeyedService<ICheckpointStore>(Listing);
        const string streamId = "Counter:multi-node-guard";
        const string viewJson = "{\"count\":3}";

        await views.SaveViewAsync(CounterStorageKey, streamId, viewJson, checkpoint: 3);
        await checkpoints.SaveCheckpointAsync(new ProjectionCheckpoint
        {
            ProjectionType = CounterStorageKey,
            NodeId = 0,
            LastSequencePosition = 3
        });

        using var client = host.GetTestServer().CreateClient();
        var response = await client.SendAsync(
            AuthRequest(HttpMethod.Post, $"/{Listing}/projections/CounterSummary/rebuild"));

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        var doc = await ParseJson(response);
        Assert.Equal(409, doc.RootElement.GetProperty("status").GetInt32());
        Assert.Contains("TotalInstances", doc.RootElement.GetProperty("detail").GetString(), StringComparison.Ordinal);

        var viewAfter = await views.GetViewAsync(CounterStorageKey, streamId);
        var cpAfter = await checkpoints.GetCheckpointAsync(CounterStorageKey, 0);
        Assert.Equal(viewJson, viewAfter);
        Assert.NotNull(cpAfter);
        Assert.Equal(3, cpAfter!.LastSequencePosition);
    }

    // ── Happy path + isolation ────────────────────────────────────────────────

    [Fact]
    public async Task Rebuild_HappyPath_Returns202_AndRunnerCatchesUp()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var eventStore = Server.Services.GetRequiredKeyedService<IEventStore>(Listing);
        var views = Server.Services.GetRequiredKeyedService<IViewStore>(Listing);
        var checkpoints = Server.Services.GetRequiredKeyedService<ICheckpointStore>(Listing);

        var streamId = $"Counter:{Guid.NewGuid():N}";
        for (var i = 0; i < 3; i++)
        {
            await eventStore.AppendAsync(
                streamId,
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: Meta(),
                cancellationToken: cts.Token);
        }

        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, CounterStorageKey, 2, TimeSpan.FromSeconds(10), cts.Token);

        var viewBefore = await views.GetViewAsync(CounterStorageKey, streamId, cts.Token);
        Assert.NotNull(viewBefore);

        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Post, $"/{Listing}/projections/CounterSummary/rebuild"));

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var doc = await ParseJson(response);
        var root = doc.RootElement;
        Assert.Equal(Listing, root.GetProperty("context").GetString());
        Assert.Equal("CounterSummary", root.GetProperty("projection").GetString());
        Assert.Equal(1, root.GetProperty("version").GetInt32());
        Assert.Equal(CounterStorageKey, root.GetProperty("storageKey").GetString());
        Assert.Equal("rebuilding", root.GetProperty("status").GetString());
        Assert.Contains("replaying from sequence 0", root.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);

        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, CounterStorageKey, 2, TimeSpan.FromSeconds(10), cts.Token);

        var viewAfter = await views.GetViewAsync(CounterStorageKey, streamId, cts.Token);
        Assert.NotNull(viewAfter);
        var view = JsonSerializer.Deserialize<CounterView>(viewAfter!, ProjectionViewJson.Read);
        Assert.Equal(3, view!.Count);
    }

    [Fact]
    public async Task Rebuild_Listing_DoesNotTouchInboundStores()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var listingEs = Server.Services.GetRequiredKeyedService<IEventStore>(Listing);
        var inboundEs = Server.Services.GetRequiredKeyedService<IEventStore>(Inbound);
        var listingCp = Server.Services.GetRequiredKeyedService<ICheckpointStore>(Listing);
        var inboundViews = Server.Services.GetRequiredKeyedService<IViewStore>(Inbound);
        var inboundCp = Server.Services.GetRequiredKeyedService<ICheckpointStore>(Inbound);

        var listingStream = $"Counter:{Guid.NewGuid():N}";
        var inboundStream = $"Counter:{Guid.NewGuid():N}";

        for (var i = 0; i < 3; i++)
        {
            await listingEs.AppendAsync(
                listingStream,
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: Meta(),
                cancellationToken: cts.Token);
            await inboundEs.AppendAsync(
                inboundStream,
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)],
                metadata: Meta(),
                cancellationToken: cts.Token);
        }

        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            listingCp, CounterStorageKey, 2, TimeSpan.FromSeconds(10), cts.Token);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            inboundCp, CounterStorageKey, 2, TimeSpan.FromSeconds(10), cts.Token);

        var inboundViewBefore = await inboundViews.GetViewAsync(CounterStorageKey, inboundStream, cts.Token);
        var inboundCpBefore = await inboundCp.GetCheckpointAsync(CounterStorageKey, 0, cts.Token);
        Assert.NotNull(inboundViewBefore);
        Assert.NotNull(inboundCpBefore);

        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Post, $"/{Listing}/projections/CounterSummary/rebuild"));
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        var inboundViewAfter = await inboundViews.GetViewAsync(CounterStorageKey, inboundStream, cts.Token);
        var inboundCpAfter = await inboundCp.GetCheckpointAsync(CounterStorageKey, 0, cts.Token);
        Assert.Equal(inboundViewBefore, inboundViewAfter);
        Assert.Equal(inboundCpBefore!.LastSequencePosition, inboundCpAfter!.LastSequencePosition);
    }

    [Fact]
    public async Task Rebuild_WithVersionQuery_ResolvesSpecificVersion()
    {
        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Post, $"/{Listing}/projections/VersionedCounterSummary/rebuild?version=1"));

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var doc = await ParseJson(response);
        Assert.Equal(1, doc.RootElement.GetProperty("version").GetInt32());
        Assert.Equal("VersionedCounterSummary:v1", doc.RootElement.GetProperty("storageKey").GetString());
    }

    [Fact]
    public async Task EnableRebuildEndpoints_False_DoesNotMapRoutes()
    {
        await using var host = await CreateHostAsync(
            contextNames: [Listing],
            configureAdmin: opts =>
            {
                opts.RequireAuthentication = false;
                opts.EnableRebuildEndpoints = false;
            });

        using var client = host.GetTestServer().CreateClient();
        var response = await client.PostAsync($"/{Listing}/projections/CounterSummary/rebuild", null);
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);

        var registration = host.Services.GetRequiredService<ProjectionAdminApiRegistration>();
        Assert.False(registration.IsRebuildMapped(Listing));
    }

    // ── Host helpers ──────────────────────────────────────────────────────────

    private static WebApplicationBuilder CreateBaseBuilder()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        builder.Services.AddLawnDart(o =>
        {
            o.RequireTenantId = false;
            o.EnableAuthorization = false;
        });
        builder.Services.AddHttpContextAccessor();
        builder.Services.AddSingleton<IAuthorizationProvider, DefaultAuthorizationProvider>();
        builder.Services.AddSingleton<AuthorizationService>();
        return builder;
    }

    /// <summary>
    /// Registers bounded contexts with a filtered projection set (avoids starting every
    /// fixture projection in the test assembly).
    /// </summary>
    private static void RegisterContexts(
        IServiceCollection services,
        IReadOnlyList<string> contextNames,
        bool multiNode)
    {
        foreach (var contextName in contextNames)
        {
            services.AddInMemoryProjectionStores(contextName);
            var builder = services.AddBoundedContext(contextName).UseInMemory();
            RegisterProjectionsForTests(builder, TestRegistrations, multiNode);
        }
    }

    private static void RegisterProjectionsForTests(
        BoundedContextBuilder builder,
        IReadOnlyList<ProjectionRegistration> registrations,
        bool multiNode)
    {
        var services = builder.Services;
        var contextName = builder.ContextName;
        var options = new LightweightProjectionOptions
        {
            ContextName = contextName,
            PollInterval = TimeSpan.FromMilliseconds(10),
            CheckpointInterval = 1,
            BatchSize = 50,
            TotalInstances = multiNode ? 2 : 1,
            NodeInstance = 0
        };

        services.AddHttpContextAccessor();
        services.AddKeyedSingleton<IPartitioningService>(contextName, (_, _) =>
            options.TotalInstances > 1
                ? (IPartitioningService)new ConsistentHashPartitioningService(options.NodeInstance, options.TotalInstances)
                : new SingleNodePartitioningService());

        services.AddKeyedSingleton<IReadOnlyList<ProjectionRegistration>>(
            contextName, (_, _) => registrations);
        services.AddKeyedSingleton<ProjectionRegistrationCatalog>(contextName, (_, _) =>
            new ProjectionRegistrationCatalog(registrations));

        services.AddKeyedSingleton<BoundedContextProjectionRunnerManager>(contextName, (sp, _) =>
            new BoundedContextProjectionRunnerManager(
                registrations,
                reg => new LightweightProjectionRunnerService(
                    reg,
                    sp.GetRequiredKeyedService<IEventStore>(contextName),
                    sp.GetRequiredKeyedService<IViewStore>(contextName),
                    sp.GetRequiredKeyedService<ICheckpointStore>(contextName),
                    sp.GetRequiredKeyedService<IPartitioningService>(contextName),
                    options,
                    sp.GetService<ILogger<LightweightProjectionRunnerService>>()),
                sp.GetService<ILogger<BoundedContextProjectionRunnerManager>>()));

        services.AddKeyedSingleton<IProjectionRunnerManager>(contextName,
            (sp, _) => sp.GetRequiredKeyedService<BoundedContextProjectionRunnerManager>(contextName));

        services.AddSingleton<IHostedService>(sp =>
            new BoundedContextRunnerManagerHostedService(
                sp.GetRequiredKeyedService<BoundedContextProjectionRunnerManager>(contextName)));

        services.AddKeyedSingleton<IProjectionAdmin>(contextName, (sp, _) =>
            new BoundedContextProjectionAdmin(
                sp.GetRequiredKeyedService<IProjectionRunnerManager>(contextName),
                sp.GetRequiredKeyedService<ICheckpointStore>(contextName),
                sp.GetRequiredKeyedService<IViewStore>(contextName),
                sp.GetRequiredKeyedService<ProjectionRegistrationCatalog>(contextName),
                sp.GetRequiredKeyedService<IPartitioningService>(contextName),
                sp.GetService<ILogger<BoundedContextProjectionAdmin>>()));

        services.TryAddSingleton<ProjectionAdminApiRegistration>();
    }

    private static async Task<WebApplication> CreateHostAsync(
        IReadOnlyList<string> contextNames,
        bool multiNode = false,
        Action<IServiceCollection>? configureAuth = null,
        Action<ProjectionAdminApiOptions>? configureAdmin = null)
    {
        var builder = CreateBaseBuilder();
        RegisterContexts(builder.Services, contextNames, multiNode);

        builder.Services
            .AddAuthentication(TestScheme)
            .AddScheme<AuthenticationSchemeOptions, FakeDebugAuthHandler>(TestScheme, _ => { });

        if (configureAuth is not null)
            configureAuth(builder.Services);
        else
            builder.Services.AddAuthorization();

        var app = builder.Build();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapProjectionAdminApi(configureAdmin ?? (opts => opts.RequireAuthentication = true));
        await app.StartAsync();
        return app;
    }

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static HttpRequestMessage AuthRequest(
        HttpMethod method,
        string url,
        params Claim[] extraClaims)
    {
        var request = new HttpRequestMessage(method, url);
        var pairs = new List<object> { new { type = "tenant_id", value = "acme" } };
        foreach (var claim in extraClaims)
            pairs.Add(new { type = claim.Type, value = claim.Value });

        var json = JsonSerializer.Serialize(pairs);
        var token = Convert.ToBase64String(Encoding.UTF8.GetBytes(json));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        return request;
    }

    private static async Task<JsonDocument> ParseJson(HttpResponseMessage response)
    {
        var body = await response.Content.ReadAsStringAsync();
        return JsonDocument.Parse(body);
    }
}
