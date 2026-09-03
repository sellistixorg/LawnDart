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
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Freshness headers and <c>minSequence</c> fail-closed behavior on MapProjectionQueries.
/// </summary>
[Trait("Category", "Integration")]
public sealed class MapProjectionFreshnessTests : IAsyncLifetime
{
    private const string TestScheme = "FakeScheme";
    private WebApplication? _app;
    private HttpClient? _client;
    private IViewStore Views => _app!.Services.GetRequiredService<IViewStore>();

    public async Task InitializeAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        builder.Services.AddHttpContextAccessor();
        builder.Services.AddSingleton<IViewStore, InMemoryViewStore>();
        builder.Services.AddSingleton<IPartitioningService, SingleNodePartitioningService>();
        builder.Services.AddSingleton<IAuthorizationProvider, DefaultAuthorizationProvider>();
        builder.Services.AddSingleton<AuthorizationService>();
        builder.Services
            .AddAuthentication(TestScheme)
            .AddScheme<AuthenticationSchemeOptions, FakeAuthHandler>(TestScheme, _ => { });
        builder.Services.AddAuthorization();

        builder.Services.AddSingleton<IReadOnlyList<ProjectionRegistration>>(
        [
            new(
                handlerType: typeof(CounterSummaryProjection),
                viewType: typeof(CounterView),
                projectionName: "CounterSummary",
                kind: ProjectionKind.SingleStream,
                tenantScope: TenantScope.TenantScoped,
                streamType: "Counter",
                endpoint: new ProjectionEndpointAttribute(
                    route: "/api/views/counters/{counterId}",
                    requiredPermission: "Counter.View"))
        ]);

        _app = builder.Build();
        _app.UseAuthentication();
        _app.UseAuthorization();
        _app.MapProjectionQueries();
        await _app.StartAsync();
        _client = _app.GetTestServer().CreateClient();
    }

    public async Task DisposeAsync()
    {
        _client?.Dispose();
        if (_app is not null)
            await _app.StopAsync();
    }

    [Fact]
    public async Task Success_IncludesSequenceAndReadSourceHeaders()
    {
        await Views.SaveViewAsync(
            "CounterSummary:v1", "tenant1:Counter:fresh",
            JsonSerializer.Serialize(new CounterView { Count = 3 }),
            checkpoint: 42);

        var response = await SendAsync("/api/views/counters/fresh", "tenant1");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("42", response.Headers.GetValues(ProjectionFreshness.SequenceHeader).Single());
        Assert.Equal(
            ProjectionFreshness.ReadSourceMemory,
            response.Headers.GetValues(ProjectionFreshness.ReadSourceHeader).Single());

        var body = await response.Content.ReadAsStringAsync();
        var view = JsonSerializer.Deserialize<CounterView>(body,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        Assert.Equal(3, view!.Count);
    }

    [Fact]
    public async Task MinSequence_Equal_Returns200()
    {
        await Views.SaveViewAsync(
            "CounterSummary:v1", "tenant1:Counter:eq",
            JsonSerializer.Serialize(new CounterView { Count = 1 }),
            checkpoint: 100);

        var response = await SendAsync("/api/views/counters/eq?minSequence=100", "tenant1");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("100", response.Headers.GetValues(ProjectionFreshness.SequenceHeader).Single());
    }

    [Fact]
    public async Task MinSequence_Behind_Returns409_WithoutStaleBody()
    {
        await Views.SaveViewAsync(
            "CounterSummary:v1", "tenant1:Counter:stale",
            JsonSerializer.Serialize(new CounterView { Count = 7 }),
            checkpoint: 50);

        var response = await SendAsync("/api/views/counters/stale?minSequence=60", "tenant1");
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);

        Assert.False(response.Headers.Contains(ProjectionFreshness.SequenceHeader));

        var body = await response.Content.ReadAsStringAsync();
        Assert.DoesNotContain("\"count\":7", body, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("\"Count\":7", body, StringComparison.Ordinal);

        using var doc = JsonDocument.Parse(body);
        Assert.Equal(409, doc.RootElement.GetProperty("status").GetInt32());
        Assert.Equal(
            ProjectionFreshness.NotCaughtUpCode,
            doc.RootElement.GetProperty("code").GetString());
    }

    [Fact]
    public async Task MinSequence_Absent_DoesNotRequireFreshness()
    {
        await Views.SaveViewAsync(
            "CounterSummary:v1", "tenant1:Counter:any",
            JsonSerializer.Serialize(new CounterView { Count = 2 }),
            checkpoint: 1);

        var response = await SendAsync("/api/views/counters/any", "tenant1");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private async Task<HttpResponseMessage> SendAsync(string path, string tenantId)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, path);
        var claimsJson = JsonSerializer.Serialize(new[]
        {
            new { type = "tenant_id", value = tenantId },
            new { type = ClaimTypes.Role, value = "Admin" }
        });
        request.Headers.Authorization = new AuthenticationHeaderValue(
            "Bearer", Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(claimsJson)));
        return await _client!.SendAsync(request);
    }
}
