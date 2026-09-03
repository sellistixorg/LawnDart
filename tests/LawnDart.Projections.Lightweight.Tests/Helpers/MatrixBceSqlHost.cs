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
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Integration;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Helpers;

/// <summary>Shared TestServer host + SQL store helpers for Matrix B/C/E SQL e2e.</summary>
internal static class MatrixBceSqlHost
{
    public const string FakeScheme = "FakeScheme";
    public const string CounterStorageKey = "CounterSummary:v1";
    public const string GlobalStorageKey = "GlobalTagIndex:v1";

    public static ProjectionRegistration MakeCounterReg() =>
        new(
            handlerType: typeof(CounterSummaryProjection),
            viewType: typeof(CounterView),
            projectionName: "CounterSummary",
            kind: ProjectionKind.SingleStream,
            tenantScope: TenantScope.TenantScoped,
            streamType: "Counter",
            endpoint: new ProjectionEndpointAttribute("/api/views/counters/{counterId}", "Counter.View"));

    public static ProjectionRegistration MakeGlobalReg() =>
        new(
            handlerType: typeof(GlobalTagIndexProjection),
            viewType: typeof(GlobalIndexView),
            projectionName: "GlobalTagIndex",
            kind: ProjectionKind.Global,
            tenantScope: TenantScope.SystemGlobal,
            endpoint: new ProjectionEndpointAttribute("/api/views/tags"));

    public static LightweightProjectionOptions AggressiveOptions(
        ProjectionReadCacheMode cacheMode = ProjectionReadCacheMode.Hot,
        ProjectionWorkingSetMode workingSet = ProjectionWorkingSetMode.EagerRestore,
        int workingSetMax = 0) =>
        new()
        {
            PollInterval = TimeSpan.FromMilliseconds(15),
            CheckpointInterval = 1,
            BatchSize = 50,
            SkipTailFlushWhileCatchingUp = false,
            ReadCacheMode = cacheMode,
            WorkingSetMode = workingSet,
            WorkingSetMaxInstances = workingSetMax
        };

    public static async Task<(SqlViewStore Views, SqlCheckpointStore Checkpoints)> CreateSqlStoresAsync(
        MsSqlMatrixAFixture fx, string schema, CancellationToken ct)
    {
        var views = new SqlViewStore(fx.ConnectionString, schemaName: schema);
        var checkpoints = new SqlCheckpointStore(fx.ConnectionString, views, schemaName: schema);
        await views.InitializeSchemaAsync(ct);
        await checkpoints.InitializeSchemaAsync(ct);
        return (views, checkpoints);
    }

    public static async Task<(WebApplication App, HttpClient Client)> StartQueryHostAsync(
        IViewStore views,
        IReadOnlyList<ProjectionRegistration> regs,
        IProjectionReadCache? cache = null,
        IProjectionRunnerManager? manager = null,
        IPartitioningService? partitioning = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        builder.Services.AddHttpContextAccessor();
        builder.Services.AddSingleton(views);
        if (cache is not null)
            builder.Services.AddSingleton(cache);
        if (manager is not null)
            builder.Services.AddSingleton(manager);
        builder.Services.AddSingleton<IPartitioningService>(
            partitioning ?? new SingleNodePartitioningService());
        builder.Services.AddSingleton<IAuthorizationProvider, DefaultAuthorizationProvider>();
        builder.Services.AddSingleton<AuthorizationService>();
        builder.Services
            .AddAuthentication(FakeScheme)
            .AddScheme<AuthenticationSchemeOptions, FakeAuthHandler>(FakeScheme, _ => { });
        builder.Services.AddAuthorization();
        builder.Services.AddSingleton<IReadOnlyList<ProjectionRegistration>>(regs);

        var app = builder.Build();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapProjectionQueries();
        await app.StartAsync();
        return (app, app.GetTestServer().CreateClient());
    }

    public static async Task<HttpResponseMessage> GetAsync(
        HttpClient client, string path, string? tenantId = "tenant1", bool authenticated = true)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, path);
        if (authenticated)
        {
            var claims = new List<object>
            {
                new { type = ClaimTypes.Role, value = "Admin" }
            };
            if (tenantId is not null)
                claims.Insert(0, new { type = "tenant_id", value = tenantId });

            var claimsJson = JsonSerializer.Serialize(claims);
            request.Headers.Authorization = new AuthenticationHeaderValue(
                "Bearer", Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(claimsJson)));
        }

        return await client.SendAsync(request);
    }
}
