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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Authorization;
using LawnDart.EventSourcing;
using LawnDart.EventStore;
using LawnDart.Projections.Lightweight.Debug;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Lightweight.TimeTravelQuery;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// In-process TestHost tests for the HTTP endpoints registered by <c>MapProjectionDebugApi()</c>.
/// </summary>
[Trait("Category", "Integration")]
public class MapProjectionDebugApiTests : IAsyncLifetime
{
    private WebApplication? _app;
    private HttpClient? _client;

    private TestServer Server => _app!.GetTestServer();
    private HttpClient Client => _client!;

    private const string TestScheme = "FakeDebugScheme";
    private const string Prefix     = "/internal/projections/debug";

    public async Task InitializeAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        builder.Services.AddLawnDart(o => { o.RequireTenantId = false; o.EnableAuthorization = false; });
        builder.Services.AddBoundedContext("default").UseInMemory();
        // Bridge for GetRequiredService<IEventStore> in timeline/state/step tests + debug DI.
        builder.Services.AddSingleton<IEventStore>(sp =>
            sp.GetRequiredKeyedService<IEventStore>("default"));
        builder.Services.AddInMemoryProjectionStores();
        builder.Services.AddHttpContextAccessor();

        builder.Services.AddSingleton<IAuthorizationProvider, DefaultAuthorizationProvider>();
        builder.Services.AddSingleton<AuthorizationService>();

        builder.Services
            .AddAuthentication(TestScheme)
            .AddScheme<AuthenticationSchemeOptions, FakeDebugAuthHandler>(TestScheme, _ => { });
        builder.Services.AddAuthorization();

        // Register a minimal set of projections — no runners needed.
        var registrations = new List<ProjectionRegistration>
        {
            new(
                handlerType: typeof(CounterSummaryProjection),
                viewType: typeof(CounterView),
                projectionName: "CounterSummary",
                kind: ProjectionKind.SingleStream,
                tenantScope: TenantScope.TenantScoped,
                streamType: "Counter"),
            new(
                handlerType: typeof(GlobalTagIndexProjection),
                viewType: typeof(GlobalIndexView),
                projectionName: "GlobalTagIndex",
                kind: ProjectionKind.Global,
                tenantScope: TenantScope.SystemGlobal),
            new(
                handlerType: typeof(GlobalTagListProjection),
                viewType: typeof(List<string>),
                projectionName: "GlobalTagList",
                kind: ProjectionKind.Global,
                tenantScope: TenantScope.SystemGlobal)
        };
        builder.Services.AddSingleton<IReadOnlyList<ProjectionRegistration>>(registrations);
        builder.Services.AddSingleton<AdHocProjectionBuilder>();
        builder.Services.AddProjectionDebugServices();

        _app = builder.Build();
        _app.UseAuthentication();
        _app.UseAuthorization();
        _app.MapProjectionDebugApi(opts =>
        {
            opts.RoutePrefix           = Prefix;
            opts.RequireAuthentication = true;
        });

        await _app.StartAsync();
        _client = Server.CreateClient();
    }

    public async Task DisposeAsync()
    {
        _client?.Dispose();
        if (_app is not null)
            await _app.StopAsync();
    }

    // ── GET /projections ──────────────────────────────────────────────────────

    [Fact]
    public async Task GetProjections_Returns200_WithRegisteredProjections()
    {
        var response = await Client.SendAsync(AuthRequest(HttpMethod.Get, $"{Prefix}/projections", tenantId: "acme"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc  = await ParseJson(response);
        var count = doc.RootElement.GetProperty("count").GetInt32();
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task GetProjections_Unauthenticated_Returns401()
    {
        var response = await Client.SendAsync(new HttpRequestMessage(HttpMethod.Get, $"{Prefix}/projections"));
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    // ── GET /projections/{name}/instances ─────────────────────────────────────

    [Fact]
    public async Task GetInstances_Returns200_EmptyPage_WhenNoViewsExist()
    {
        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Get, $"{Prefix}/projections/CounterSummary/instances", tenantId: "acme"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc = await ParseJson(response);
        Assert.Equal(0, doc.RootElement.GetProperty("totalFiltered").GetInt32());
    }

    [Fact]
    public async Task GetInstances_Returns200_PagedResults_FilteredByTenant()
    {
        var viewStore = Server.Services.GetRequiredService<IViewStore>();
        await viewStore.SaveViewAsync("CounterSummary:v1", "acme:Counter:1", "{}", 1);
        await viewStore.SaveViewAsync("CounterSummary:v1", "acme:Counter:2", "{}", 2);
        await viewStore.SaveViewAsync("CounterSummary:v1", "other:Counter:3", "{}", 3);

        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Get, $"{Prefix}/projections/CounterSummary/instances?pageSize=10", tenantId: "acme"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc = await ParseJson(response);
        Assert.Equal(2, doc.RootElement.GetProperty("totalFiltered").GetInt32());

        var items = doc.RootElement.GetProperty("items").EnumerateArray().Select(e => e.GetString()).ToList();
        Assert.All(items, id => Assert.StartsWith("acme:", id));
    }

    [Fact]
    public async Task GetInstances_Returns401_WhenTenantClaimMissing_ForTenantScopedProjection()
    {
        // No tenant claim — TenantScoped projection should reject with 401.
        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Get, $"{Prefix}/projections/CounterSummary/instances", tenantId: null));

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task GetInstances_SystemGlobal_Returns200_WithoutTenantClaim()
    {
        // GlobalTagIndex is SystemGlobal — no tenant claim required.
        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Get, $"{Prefix}/projections/GlobalTagIndex/instances", tenantId: null));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task GetInstances_UnknownProjection_Returns400()
    {
        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Get, $"{Prefix}/projections/DoesNotExist/instances", tenantId: "acme"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    // ── GET /projections/{name}/instances/{id}/timeline ───────────────────────

    [Fact]
    public async Task GetTimeline_Returns200_WithAppliedEvents()
    {
        // Append 3 events so the timeline has 3 entries.
        var eventStore = Server.Services.GetRequiredService<LawnDart.EventStore.IEventStore>();
        const string streamId = "tenant1:Counter:tl1";
        var meta = new LawnDart.Metadata.EventMetadata { UserId = "test" };
        await eventStore.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)], metadata: meta);
        await eventStore.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 2)], metadata: meta);
        await eventStore.AppendAsync(streamId, [new CounterReset(Guid.NewGuid(), DateTime.UtcNow)], metadata: meta);

        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Get, $"{Prefix}/projections/CounterSummary/instances/{Uri.EscapeDataString(streamId)}/timeline?pageSize=50", tenantId: "tenant1"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc    = await ParseJson(response);
        var events = doc.RootElement.GetProperty("events").EnumerateArray().ToList();
        Assert.Equal(3, events.Count);
        Assert.Equal(0, events[0].GetProperty("appliedIndex").GetInt64());
        Assert.Equal(2, events[2].GetProperty("appliedIndex").GetInt64());
    }

    [Fact]
    public async Task GetTimeline_UnknownProjection_Returns400()
    {
        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Get, $"{Prefix}/projections/NoSuch/instances/global/timeline", tenantId: "x"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    // ── GET /projections/{name}/instances/{id}/state ──────────────────────────

    [Fact]
    public async Task GetState_AtSequence_Returns200_WithCorrectView()
    {
        var eventStore = Server.Services.GetRequiredService<LawnDart.EventStore.IEventStore>();
        const string streamId = "tenant1:Counter:seq1";
        var meta = new LawnDart.Metadata.EventMetadata { UserId = "test" };
        await eventStore.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 10)], metadata: meta);
        await eventStore.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 5)], metadata: meta);

        // Versions are 1-indexed; version 1 = the first event.
        var seqAfterFirst = (await eventStore.ReadStreamAsync(streamId, toVersion: 1)).First().SequencePosition;

        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Get,
                $"{Prefix}/projections/CounterSummary/instances/{Uri.EscapeDataString(streamId)}/state?atSequence={seqAfterFirst}",
                tenantId: "tenant1"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc = await ParseJson(response);
        Assert.Equal(1, doc.RootElement.GetProperty("eventsApplied").GetInt64());
    }

    [Fact]
    public async Task GetState_AtAppliedIndex_Returns200()
    {
        var eventStore = Server.Services.GetRequiredService<LawnDart.EventStore.IEventStore>();
        const string streamId = "tenant1:Counter:idx1";
        var meta = new LawnDart.Metadata.EventMetadata { UserId = "test" };
        await eventStore.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 3)], metadata: meta);
        await eventStore.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 7)], metadata: meta);

        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Get,
                $"{Prefix}/projections/CounterSummary/instances/{Uri.EscapeDataString(streamId)}/state?atAppliedIndex=0",
                tenantId: "tenant1"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc = await ParseJson(response);
        Assert.Equal(1, doc.RootElement.GetProperty("eventsApplied").GetInt64());
    }

    [Fact]
    public async Task GetState_NoCutoff_Returns400()
    {
        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Get,
                $"{Prefix}/projections/CounterSummary/instances/some-id/state",
                tenantId: "x"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task GetState_MultipleCutoffs_Returns400()
    {
        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Get,
                $"{Prefix}/projections/CounterSummary/instances/some-id/state?atSequence=1&atVersion=1",
                tenantId: "x"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task GetState_UnknownProjection_Returns400()
    {
        var response = await Client.SendAsync(
            AuthRequest(HttpMethod.Get,
                $"{Prefix}/projections/DoesNotExist/instances/id/state?atSequence=1",
                tenantId: "x"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    // ── POST /projections/{name}/instances/{id}/step ──────────────────────────

    [Fact]
    public async Task PostStep_Forward_Returns200_WithDiff()
    {
        var eventStore = Server.Services.GetRequiredService<LawnDart.EventStore.IEventStore>();
        const string streamId = "tenant1:Counter:step1";
        var meta = new LawnDart.Metadata.EventMetadata { UserId = "test" };
        await eventStore.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 5)], metadata: meta);
        await eventStore.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 3)], metadata: meta);

        var request = AuthRequest(
            HttpMethod.Post,
            $"{Prefix}/projections/CounterSummary/instances/{Uri.EscapeDataString(streamId)}/step",
            tenantId: "tenant1");
        request.Content = JsonContent(new { direction = "forward", currentAppliedIndex = -1 });

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc = await ParseJson(response);
        Assert.Equal(0, doc.RootElement.GetProperty("appliedIndex").GetInt64());
        // Diff should be non-null (forward from empty produces changes).
        Assert.NotEqual(JsonValueKind.Null, doc.RootElement.GetProperty("diff").ValueKind);
        Assert.Equal(nameof(CounterIncremented), doc.RootElement.GetProperty("event").GetProperty("eventType").GetString());
        Assert.Equal(5, doc.RootElement.GetProperty("event").GetProperty("eventJson").GetProperty("Amount").GetInt32());
    }

    [Fact]
    public async Task PostStep_ListBackedProjection_ReturnsArrayAwareDiffPayload()
    {
        var eventStore = Server.Services.GetRequiredService<LawnDart.EventStore.IEventStore>();
        var meta = new LawnDart.Metadata.EventMetadata { UserId = "test" };
        await eventStore.AppendAsync("tag-source-1", [new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "alpha")], metadata: meta);
        await eventStore.AppendAsync("tag-source-2", [new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "beta")], metadata: meta);

        var request = AuthRequest(
            HttpMethod.Post,
            $"{Prefix}/projections/GlobalTagList/instances/global/step",
            tenantId: null);
        request.Content = JsonContent(new { direction = "forward", currentAppliedIndex = 0 });

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var doc = await ParseJson(response);
        var diff = doc.RootElement.GetProperty("diff");

        Assert.Equal(1, doc.RootElement.GetProperty("appliedIndex").GetInt64());
        Assert.Equal("alpha", doc.RootElement.GetProperty("state").GetProperty("viewState")[0].GetString());
        Assert.Equal("beta", doc.RootElement.GetProperty("state").GetProperty("viewState")[1].GetString());

        var addedPaths = diff.GetProperty("addedPaths").EnumerateArray().Select(x => x.GetString()).ToList();
        Assert.Contains("[1]", addedPaths);
        Assert.Empty(diff.GetProperty("removedPaths").EnumerateArray());
        Assert.Empty(diff.GetProperty("changedProperties").EnumerateArray());
    }

    [Fact]
    public async Task PostStep_Back_Returns200_WithDiff()
    {
        var eventStore = Server.Services.GetRequiredService<LawnDart.EventStore.IEventStore>();
        const string streamId = "tenant1:Counter:back1";
        var meta = new LawnDart.Metadata.EventMetadata { UserId = "test" };
        await eventStore.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 10)], metadata: meta);
        await eventStore.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 20)], metadata: meta);

        var request = AuthRequest(
            HttpMethod.Post,
            $"{Prefix}/projections/CounterSummary/instances/{Uri.EscapeDataString(streamId)}/step",
            tenantId: "tenant1");
        request.Content = JsonContent(new { direction = "back", currentAppliedIndex = 1 });

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc = await ParseJson(response);
        Assert.Equal(0, doc.RootElement.GetProperty("appliedIndex").GetInt64());
    }

    [Fact]
    public async Task PostStep_AtBoundary_StepBackBeyondZero_ReturnsEmptyState()
    {
        var eventStore = Server.Services.GetRequiredService<LawnDart.EventStore.IEventStore>();
        const string streamId = "tenant1:Counter:boundary";
        var meta = new LawnDart.Metadata.EventMetadata { UserId = "test" };
        await eventStore.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 5)], metadata: meta);

        // Step back from index 0 → targetIndex = -1 → empty state.
        var request = AuthRequest(
            HttpMethod.Post,
            $"{Prefix}/projections/CounterSummary/instances/{Uri.EscapeDataString(streamId)}/step",
            tenantId: "tenant1");
        request.Content = JsonContent(new { direction = "back", currentAppliedIndex = 0 });

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var doc = await ParseJson(response);
        Assert.Equal(-1, doc.RootElement.GetProperty("appliedIndex").GetInt64());
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("event").ValueKind);
    }

    [Fact]
    public async Task PostStep_InvalidDirection_Returns400()
    {
        var request = AuthRequest(
            HttpMethod.Post,
            $"{Prefix}/projections/CounterSummary/instances/some/step",
            tenantId: "x");
        request.Content = JsonContent(new { direction = "sideways", currentAppliedIndex = 0 });

        var response = await Client.SendAsync(request);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task PostStep_Unauthenticated_Returns401()
    {
        var request = new HttpRequestMessage(HttpMethod.Post,
            $"{Prefix}/projections/CounterSummary/instances/some/step");
        request.Content = JsonContent(new { direction = "forward", currentAppliedIndex = 0 });

        var response = await Client.SendAsync(request);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static HttpRequestMessage AuthRequest(HttpMethod method, string url, string? tenantId)
    {
        var request = new HttpRequestMessage(method, url);
        var pairs   = new List<KeyValuePair<string, string>>();

        if (tenantId is not null)
            pairs.Add(new("tenant_id", tenantId));

        var json  = JsonSerializer.Serialize(pairs.Select(p => new { type = p.Key, value = p.Value }));
        var token = Convert.ToBase64String(Encoding.UTF8.GetBytes(json));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        return request;
    }

    private static StringContent JsonContent(object payload) =>
        new(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");

    private static async Task<JsonDocument> ParseJson(HttpResponseMessage response)
    {
        var body = await response.Content.ReadAsStringAsync();
        return JsonDocument.Parse(body);
    }
}

// ── Fake authentication handler ───────────────────────────────────────────────

internal sealed class FakeDebugAuthHandler : AuthenticationHandler<AuthenticationSchemeOptions>
{
    public FakeDebugAuthHandler(
        IOptionsMonitor<AuthenticationSchemeOptions> options,
        ILoggerFactory logger,
        System.Text.Encodings.Web.UrlEncoder encoder)
        : base(options, logger, encoder) { }

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var header = Request.Headers.Authorization.ToString();

        if (string.IsNullOrEmpty(header) || !header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
            return Task.FromResult(AuthenticateResult.NoResult());

        var token = header["Bearer ".Length..].Trim();

        try
        {
            var json       = Encoding.UTF8.GetString(Convert.FromBase64String(token));
            var claimsData = JsonSerializer.Deserialize<List<ClaimItem>>(json,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? [];

            var claims = claimsData
                .Select(c => new Claim(c.Type, c.Value))
                .Append(new Claim(ClaimTypes.Name, "test-user"))
                .ToList();

            var identity  = new ClaimsIdentity(claims, Scheme.Name);
            var principal = new ClaimsPrincipal(identity);
            var ticket    = new AuthenticationTicket(principal, Scheme.Name);

            return Task.FromResult(AuthenticateResult.Success(ticket));
        }
        catch
        {
            return Task.FromResult(AuthenticateResult.Fail("Invalid test token"));
        }
    }

    private record ClaimItem(string Type, string Value);
}
