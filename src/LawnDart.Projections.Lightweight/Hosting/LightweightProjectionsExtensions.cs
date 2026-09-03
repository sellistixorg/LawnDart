using System.Reflection;
using System.Text.Json;
using System.Globalization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using LawnDart.Authorization;
using LawnDart.EventStore;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Debug;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Security;
using LawnDart.Projections.Lightweight.TimeTravelQuery;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;
using LawnDart.Projections.Telemetry;

namespace LawnDart.Projections.Lightweight;

/// <summary>
/// Extension methods for registering the lightweight projection infrastructure and
/// mapping generated query (GET) endpoints.
/// </summary>
public static class LightweightProjectionsExtensions
{
    /// <summary>
    /// Registers convenience in-memory implementations of <see cref="IViewStore"/> and
    /// <see cref="ICheckpointStore"/> suitable for development, testing, and demo scenarios.
    /// Data is not persisted across application restarts.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddInMemoryProjectionStores(
        this IServiceCollection services)
    {
        services.TryAddSingleton<IViewStore, InMemoryViewStore>();
        services.TryAddSingleton<ICheckpointStore>(sp =>
            new InMemoryCheckpointStore(
                sp.GetRequiredService<IViewStore>(),
                sp.GetService<Microsoft.Extensions.Logging.ILogger<InMemoryCheckpointStore>>()));
        return services;
    }

    /// <summary>
    /// Maps auto-generated <c>GET</c> Minimal API endpoints for every projection class
    /// decorated with <see cref="ProjectionEndpointAttribute"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// For <see cref="TenantScope.TenantScoped"/> projections the tenant ID is extracted
    /// from the authenticated JWT (<c>tenant_id</c> / <c>tid</c> claim) and used to
    /// construct the view instance key.  The request URL is <em>never</em> used as the
    /// source of the tenant identifier, preventing cross-tenant data access.
    /// </para>
    /// <para>
    /// Each generated endpoint:
    /// <list type="bullet">
    ///   <item>Requires an authenticated request (returns 401 otherwise).</item>
    ///   <item>Optionally enforces a permission via <see cref="IAuthorizationProvider"/>
    ///         (returns 403 on failure).</item>
    ///   <item>Returns 404 when no view exists for the resolved instance key.</item>
    ///   <item>Returns 200 with <c>application/json</c> body on success.</item>
    ///   <item>Adds <c>Cache-Control: max-age=N</c> when
    ///         <see cref="ProjectionEndpointAttribute.CacheMaxAgeSeconds"/> &gt; 0.</item>
    /// </list>
    /// </para>
    /// </remarks>
    /// <param name="app">The endpoint route builder.</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    /// <example>
    /// <code>
    /// var app = builder.Build();
    /// app.UseAuthentication();
    /// app.UseAuthorization();
    /// app.MapLawnDartCommands();
    /// app.MapProjectionQueries();   // registers GET /api/views/... endpoints
    /// app.Run();
    /// </code>
    /// </example>
    public static IEndpointRouteBuilder MapProjectionQueries(
        this IEndpointRouteBuilder app)
    {
        var registrations = app.ServiceProvider
            .GetService<IReadOnlyList<ProjectionRegistration>>();

        if (registrations is null || registrations.Count == 0)
            return app;

        MapProjectionQueryEndpoints(app, new ProjectionRegistrationCatalog(registrations), contextName: null);

        return app;
    }

    /// <summary>
    /// Maps auto-generated <c>GET</c> Minimal API endpoints for every projection class
    /// decorated with <see cref="ProjectionEndpointAttribute"/> that belongs to the specified
    /// bounded context(s).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this overload when projections were registered via the
    /// <c>BoundedContextBuilder.WithProjections()</c> path (keyed DI).  The method resolves
    /// <c>IReadOnlyList&lt;ProjectionRegistration&gt;</c> and <c>IViewStore</c> from the keyed
    /// DI container using the supplied context name(s) — no manual bridging of keyed services
    /// to non-keyed ones is required.
    /// </para>
    /// <para>
    /// When multiple context names are provided, each context's projections are mapped
    /// independently and each uses its own keyed <c>IViewStore</c>.
    /// </para>
    /// </remarks>
    /// <param name="app">The endpoint route builder.</param>
    /// <param name="contextName">The primary bounded context name (DI key).</param>
    /// <param name="additionalContextNames">Optional additional context names whose projections should also be mapped.</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    /// <example>
    /// <code>
    /// // Single context
    /// app.MapProjectionQueries("listing");
    ///
    /// // Multiple contexts — each uses its own keyed IViewStore
    /// app.MapProjectionQueries("listing", "catalog");
    /// </code>
    /// </example>
    public static IEndpointRouteBuilder MapProjectionQueries(
        this IEndpointRouteBuilder app,
        string contextName,
        params string[] additionalContextNames)
    {
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", nameof(contextName));

        MapProjectionQueriesForContext(app, contextName);

        foreach (var extra in additionalContextNames)
        {
            if (!string.IsNullOrWhiteSpace(extra))
                MapProjectionQueriesForContext(app, extra);
        }

        return app;
    }

    private static void MapProjectionQueriesForContext(IEndpointRouteBuilder app, string contextName)
    {
        var registrations = app.ServiceProvider
            .GetKeyedService<IReadOnlyList<ProjectionRegistration>>(contextName);

        if (registrations is null || registrations.Count == 0)
            return;

        MapProjectionQueryEndpoints(app, new ProjectionRegistrationCatalog(registrations), contextName);
    }

    private static void MapProjectionQueryEndpoints(
        IEndpointRouteBuilder app,
        ProjectionRegistrationCatalog catalog,
        string? contextName)
    {
        foreach (var family in catalog.Families)
        {
            MapMetadataEndpointForFamily(app, family, contextName);

            foreach (var registration in family.Versions.Where(v => v.Endpoint is not null))
            {
                MapEndpointForRegistration(
                    app,
                    registration,
                    contextName,
                    route: BuildVersionedRoute(registration.Endpoint!.Route, registration.Version),
                    routeName: $"{registration.LogicalName}_v{registration.Version}_Query",
                    summary: $"Get version {registration.Version} of the {registration.LogicalName} projection.",
                    isLatestAlias: false);
            }

            if (family.Latest.Endpoint is not null)
            {
                MapEndpointForRegistration(
                    app,
                    family.Latest,
                    contextName,
                    route: BuildLatestAliasRoute(family.Latest.Endpoint.Route),
                    routeName: $"{family.LogicalName}_Latest_Query",
                    summary: $"Get the latest view state for the {family.LogicalName} projection.",
                    isLatestAlias: true);
            }
        }
    }

    // ── Endpoint registration per projection ──────────────────────────────────

    private static void MapEndpointForRegistration(
        IEndpointRouteBuilder app,
        ProjectionRegistration reg,
        string? contextName,
        string route,
        string routeName,
        string summary,
        bool isLatestAlias)
    {
        var endpoint = reg.Endpoint!;
        var projectionName = reg.LogicalName;
        var kind = reg.Kind;
        var streamType = reg.StreamType;
        var tenantScope = reg.TenantScope;
        var requiredPermission = endpoint.RequiredPermission;
        var cacheSeconds = endpoint.CacheMaxAgeSeconds;

        // Extract the route parameter names from the route template so we can
        // look them up in the HttpContext.Request.RouteValues at runtime.
        var routeParams = ExtractRouteParameters(route);

        // Capture context name so the per-request IViewStore resolution uses the correct keyed
        // instance.  Null means the caller is using the non-keyed (single-context) path.
        var capturedContextName = contextName;

        var handler = async (HttpContext ctx) =>
        {
            // 1. Authentication guard
            if (!ctx.User.Identity?.IsAuthenticated ?? true)
                return Results.Unauthorized();

            // 2. For TenantScoped projections, verify the tenant claim exists BEFORE
            //    running the permission check so a missing claim returns 401, not 403.
            string? earlyTenantId = null;
            if (tenantScope == TenantScope.TenantScoped)
            {
                earlyTenantId = ctx.User.FindFirst("tenant_id")?.Value
                    ?? ctx.User.FindFirst("tid")?.Value;

                if (string.IsNullOrEmpty(earlyTenantId))
                    return Results.Unauthorized();
            }

            // 3. Permission check (optional)
            if (requiredPermission is not null)
            {
                var authzProvider = ctx.RequestServices
                    .GetService<IAuthorizationProvider>();

                if (authzProvider is not null)
                {
                    var authzCtxProvider = ctx.RequestServices
                        .GetService<LawnDart.Authorization.IAuthorizationContextProvider>();

                    LawnDart.Authorization.AuthorizationContext? authzCtx = null;
                    if (authzCtxProvider is not null)
                        authzCtx = await authzCtxProvider.GetAuthorizationContextAsync(ctx.RequestAborted);

                    // Build a minimal context from claims if no provider present
                    authzCtx ??= BuildAuthContextFromClaims(ctx);

                    var result = await authzProvider.CheckPermissionAsync(
                        authzCtx, requiredPermission, ctx.RequestAborted);

                    if (!result.IsAuthorized)
                        return Results.Forbid();
                }
            }

            // 4. Resolve instance key
            string instanceId;
            switch (tenantScope)
            {
                case TenantScope.TenantScoped:
                {
                    // earlyTenantId is guaranteed non-null — checked in step 2
                    var tenantId = earlyTenantId!;

                    var entityId = ResolveEntityId(ctx, routeParams);

                    if (kind == ProjectionKind.MultiStream)
                    {
                        // Multi-stream key: {tenantId}:{entityId}  (no stream-type segment)
                        instanceId = entityId is not null
                            ? $"{tenantId}:{entityId}"
                            : $"{tenantId}:{projectionName}";
                    }
                    else
                    {
                        // Single-stream key: {tenantId}:{streamType}:{entityId}
                        instanceId = entityId is not null
                            ? $"{tenantId}:{streamType ?? projectionName}:{entityId}"
                            : $"{tenantId}:{projectionName}";
                    }
                    break;
                }

                case TenantScope.TenantGlobal:
                {
                    var entityId = ResolveEntityId(ctx, routeParams);
                    instanceId = entityId ?? projectionName;
                    break;
                }

                default: // SystemGlobal
                    if (ProjectionInstanceIds.IsUnpartitionedKind(kind))
                    {
                        // Prefer the host partitioning service (AddLightweightProjections / WithProjections).
                        // Fall back to single-node "global" when only MapProjectionQueries + stores are wired.
                        IPartitioningService? partitioning = capturedContextName is not null
                            ? ctx.RequestServices.GetKeyedService<IPartitioningService>(capturedContextName)
                            : ctx.RequestServices.GetService<IPartitioningService>();
                        instanceId = partitioning is not null
                            ? ProjectionInstanceIds.ForUnpartitioned(partitioning)
                            : ProjectionInstanceIds.UnpartitionedBase;
                    }
                    else
                    {
                        instanceId = ProjectionInstanceIds.UnpartitionedBase;
                    }
                    break;
            }

            // 5. Load view — memory (runner / hot cache) first, then durable store (Q1).
            //    Cache miss must never 404 when durable still has the row.
            var viewStore = capturedContextName is not null
                ? ctx.RequestServices.GetRequiredKeyedService<IViewStore>(capturedContextName)
                : ctx.RequestServices.GetRequiredService<IViewStore>();

            var runnerManager = capturedContextName is not null
                ? ctx.RequestServices.GetKeyedService<IProjectionRunnerManager>(capturedContextName)
                : ctx.RequestServices.GetService<IProjectionRunnerManager>();

            var readCache = capturedContextName is not null
                ? ctx.RequestServices.GetKeyedService<IProjectionReadCache>(capturedContextName)
                : ctx.RequestServices.GetService<IProjectionReadCache>();

            string? viewJson = null;
            long viewSequence = 0;
            var readSource = ProjectionFreshness.InferReadSource(viewStore);
            var cacheHit = false;

            if (runnerManager is not null
                && runnerManager.TryGetView(reg.StorageKey, instanceId, out var liveJson, out var liveSeq))
            {
                viewJson = liveJson;
                viewSequence = liveSeq;
                readSource = ProjectionFreshness.ReadSourceMemory;
                cacheHit = true;
            }
            else if (readCache is not null
                     && readCache.TryGet(reg.StorageKey, instanceId, out var cached))
            {
                viewJson = cached.ViewJson;
                viewSequence = cached.Sequence;
                readSource = ProjectionFreshness.ReadSourceMemory;
                cacheHit = true;
            }
            else
            {
                var loaded = await viewStore.GetViewWithCheckpointAsync(
                    reg.StorageKey, instanceId, ctx.RequestAborted);

                if (loaded is null)
                    return Results.NotFound();

                viewJson = loaded.Value.ViewData;
                viewSequence = loaded.Value.Checkpoint;
                readSource = ProjectionFreshness.InferReadSource(viewStore);
                readCache?.OfferFromDurable(reg.StorageKey, instanceId, viewJson, viewSequence);
            }

            // 6. Optional freshness demand — fail closed; never return a stale 200 body.
            if (ProjectionFreshness.TryParseMinSequence(ctx.Request, out var minSequence)
                && viewSequence < minSequence)
            {
                ProjectionTelemetry.RecordStaleRejected(reg.StorageKey);
                return ProjectionFreshness.NotCaughtUp(viewSequence, minSequence);
            }

            ProjectionTelemetry.RecordRead(reg.StorageKey, cacheHit, readSource);

            // 7. Optionally set cache headers
            if (cacheSeconds > 0)
                ctx.Response.Headers["Cache-Control"] = $"max-age={cacheSeconds}";

            ctx.Response.Headers["X-Projection-Logical-Name"] = reg.LogicalName;
            ctx.Response.Headers["X-Projection-Version"] = reg.Version.ToString(CultureInfo.InvariantCulture);
            ctx.Response.Headers["X-Projection-Storage-Key"] = reg.StorageKey;
            ctx.Response.Headers["X-Projection-Is-Latest"] = reg.IsLatest ? "true" : "false";
            ctx.Response.Headers["X-Projection-Latest-Resolution"] = reg.LatestResolutionMode.ToString();
            ctx.Response.Headers["X-Projection-Route-Alias"] = isLatestAlias ? "latest" : "versioned";

            if (!string.IsNullOrWhiteSpace(reg.DeprecationDateIso))
            {
                ctx.Response.Headers["Deprecation"] = reg.DeprecationDateIso!;
                ctx.Response.Headers["X-Projection-Deprecation-Date"] = reg.DeprecationDateIso!;
            }

            ProjectionFreshness.WriteSuccessHeaders(ctx.Response, viewSequence, readSource);

            return Results.Text(viewJson, "application/json");
        };

        app.MapGet(route, handler)
            .WithName(routeName)
            .WithTags("Views")
            .WithSummary(summary)
            .Produces(200, contentType: "application/json")
            .Produces(401)
            .Produces(403)
            .Produces(404)
            .Produces(409)
            .RequireAuthorization();
    }

    private static void MapMetadataEndpointForFamily(
        IEndpointRouteBuilder app,
        ProjectionLogicalFamily family,
        string? contextName)
    {
        var route = $"/api/views/metadata/{family.KebabName}";
        var capturedContextName = contextName;

        app.MapGet(route, async (HttpContext ctx) =>
        {
            if (!ctx.User.Identity?.IsAuthenticated ?? true)
                return Results.Unauthorized();

            var viewStore = capturedContextName is not null
                ? ctx.RequestServices.GetRequiredKeyedService<IViewStore>(capturedContextName)
                : ctx.RequestServices.GetRequiredService<IViewStore>();

            var versions = new List<object>(family.Versions.Count);
            foreach (var version in family.Versions.OrderBy(v => v.Version))
            {
                var endpointRoute = version.Endpoint is null ? null : BuildVersionedRoute(version.Endpoint.Route, version.Version);
                var aliasRoute = version.IsLatest && version.Endpoint is not null
                    ? BuildLatestAliasRoute(version.Endpoint.Route)
                    : null;
                var count = (await viewStore.GetViewsByTypeAsync(version.StorageKey, ctx.RequestAborted)).Count();

                versions.Add(new
                {
                    version.LogicalName,
                    version.Version,
                    version.StorageKey,
                    version.Kind,
                    TenantScope = version.TenantScope.ToString(),
                    version.IsLatest,
                    version.DeclaredAsLatest,
                    LatestResolutionMode = version.LatestResolutionMode.ToString(),
                    version.DeprecationDateIso,
                    HasEndpoint = version.Endpoint is not null,
                    VersionedRoute = endpointRoute,
                    LatestAliasRoute = aliasRoute,
                    StoredInstanceCount = count
                });
            }

            return Results.Ok(new
            {
                family.LogicalName,
                family.KebabName,
                LatestVersion = family.Latest.Version,
                LatestStorageKey = family.Latest.StorageKey,
                LatestResolutionMode = family.LatestResolutionMode.ToString(),
                Versions = versions
            });
        })
        .WithName($"{family.LogicalName}_Metadata")
        .WithTags("Views")
        .WithSummary($"Get version metadata for the {family.LogicalName} projection family.")
        .Produces(200)
        .Produces(401)
        .RequireAuthorization();
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static IReadOnlyList<string> ExtractRouteParameters(string route)
    {
        var parameters = new List<string>();
        var span = route.AsSpan();
        int start = -1;

        for (int i = 0; i < span.Length; i++)
        {
            if (span[i] == '{')
            {
                start = i + 1;
            }
            else if (span[i] == '}' && start >= 0)
            {
                var param = span[start..i].ToString().TrimEnd('?');
                if (param.Length > 0)
                    parameters.Add(param);
                start = -1;
            }
        }

        return parameters;
    }

    private static string BuildVersionedRoute(string route, int version)
    {
        if (route.Contains("{version}", StringComparison.OrdinalIgnoreCase))
            return route.Replace("{version}", version.ToString(CultureInfo.InvariantCulture), StringComparison.OrdinalIgnoreCase);

        const string defaultPrefix = "/api/views";
        if (route.StartsWith(defaultPrefix, StringComparison.OrdinalIgnoreCase))
            return $"{defaultPrefix}/v{version}{route[defaultPrefix.Length..]}";

        return $"/v{version}{route}";
    }

    private static string BuildLatestAliasRoute(string route)
    {
        var withoutVersionSegment = route
            .Replace("/v{version}", string.Empty, StringComparison.OrdinalIgnoreCase)
            .Replace("{version}/", string.Empty, StringComparison.OrdinalIgnoreCase)
            .Replace("{version}", string.Empty, StringComparison.OrdinalIgnoreCase);

        return string.IsNullOrWhiteSpace(withoutVersionSegment)
            ? route
            : withoutVersionSegment;
    }

    private static string? ResolveEntityId(
        HttpContext ctx,
        IReadOnlyList<string> routeParams)
    {
        // Join all route parameter values with ':' so that multi-parameter routes
        // (e.g. /{accountId}/{workingSetId}/detail) produce a composite entity key
        // (e.g. "1:77d4c81b-...") that, when prefixed with the tenant ID by the
        // TenantScoped path, matches the instanceId stored by the projection handler
        // (e.g. "1:1:77d4c81b-...").  Single-parameter routes are unchanged.
        var parts = new List<string>(routeParams.Count);
        foreach (var param in routeParams)
        {
            if (ctx.Request.RouteValues.TryGetValue(param, out var val) && val is not null)
                parts.Add(val.ToString()!);
        }

        return parts.Count > 0 ? string.Join(":", parts) : null;
    }

    private static LawnDart.Authorization.AuthorizationContext BuildAuthContextFromClaims(
        HttpContext ctx)
    {
        var user = ctx.User;
        return new LawnDart.Authorization.AuthorizationContext
        {
            UserId = user.FindFirst(System.Security.Claims.ClaimTypes.NameIdentifier)?.Value
                ?? user.FindFirst("sub")?.Value,
            UserRoles = user.FindAll(System.Security.Claims.ClaimTypes.Role)
                .Select(c => c.Value).ToList(),
            UserClaims = user.Claims
                .Select(c => $"{c.Type}:{c.Value}").ToList(),
            TenantId = user.FindFirst("tenant_id")?.Value
                ?? user.FindFirst("tid")?.Value
        };
    }

    // ── Projection Debug API ──────────────────────────────────────────────────

    /// <summary>
    /// Registers the <see cref="ProjectionTimelineService"/> singleton required by
    /// <c>MapProjectionDebugApi()</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this overload when projections were registered via the single-context
    /// unkeyed registration path.  The non-keyed <c>IEventStore</c>,
    /// <c>IViewStore</c>, and <c>IReadOnlyList&lt;ProjectionRegistration&gt;</c> singletons
    /// that <c>AddLightweightProjections</c> registers are resolved automatically.
    /// </para>
    /// <para>
    /// For the multi-context <c>WithProjections()</c> path use the overload that accepts
    /// a context name: <c>AddProjectionDebugServices(contextName)</c>.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <returns>A fluent builder for optional debug UX configuration.</returns>
    public static ProjectionDebugBuilder AddProjectionDebugServices(
        this IServiceCollection services)
    {
        var rootOpts = EnsureDebugOptions(services);
        var endpoint = CreateEndpointRegistration(rootOpts);
        services.TryAddSingleton<ProjectionTimelineService>();
        return new ProjectionDebugBuilder(services, rootOpts, endpoint);
    }

    /// <summary>
    /// Registers the <see cref="ProjectionTimelineService"/> singleton required by
    /// <c>MapProjectionDebugApi()</c>, bridging from the keyed DI services registered by
    /// the <c>WithProjections()</c> / <c>BoundedContextBuilder</c> path.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="primaryContextName">
    /// The bounded context whose <c>IEventStore</c> and <c>IViewStore</c> the debug service
    /// will use for event replay and instance listing.  This must match the name passed to
    /// <c>AddBoundedContext(name)</c>.
    /// </param>
    /// <param name="additionalContextNames">
    /// Optional additional context names whose <c>IReadOnlyList&lt;ProjectionRegistration&gt;</c>
    /// will be merged with <paramref name="primaryContextName"/>'s registrations so that
    /// <c>GET /projections</c> lists projections from all supplied contexts.
    /// Note that timeline replay and state queries only operate against
    /// <paramref name="primaryContextName"/>'s event store and view store.
    /// </param>
    /// <returns>A fluent builder for optional debug UX configuration.</returns>
    /// <example>
    /// <code>
    /// // Single context registered via WithProjections()
    /// builder.Services.AddProjectionDebugServices("listing");
    ///
    /// // Two contexts — list shows both, replay uses "listing" event store
    /// builder.Services.AddProjectionDebugServices("listing", "catalog");
    ///
    /// // Independent debugger per bounded context
    /// builder.Services.AddProjectionDebugServices("listing")
    ///     .WithDebuggingUX("/listing/projectiondebugger");
    /// builder.Services.AddProjectionDebugServices("inbound")
    ///     .WithDebuggingUX("/inbound/projectiondebugger");
    /// app.MapProjectionDebugApi();
    /// app.MapProjectionDebugUx();
    /// </code>
    /// </example>
    public static ProjectionDebugBuilder AddProjectionDebugServices(
        this IServiceCollection services,
        string primaryContextName,
        params string[] additionalContextNames)
    {
        if (string.IsNullOrWhiteSpace(primaryContextName))
            throw new ArgumentException("Primary context name must not be null or whitespace.", nameof(primaryContextName));

        var rootOpts = EnsureDebugOptions(services);
        var endpoint = CreateEndpointRegistration(
            rootOpts,
            debugServiceKey: primaryContextName,
            primaryContextName: primaryContextName,
            additionalContextNames: additionalContextNames);

        RegisterKeyedProjectionDebugServices(services, primaryContextName, additionalContextNames);

        if (CountContextScopedEndpoints(rootOpts) == 1 && additionalContextNames.Length == 0)
        {
            BridgeNonKeyedProjectionDebugServices(services, primaryContextName);
            services.TryAddSingleton<ProjectionTimelineService>(sp =>
                sp.GetRequiredKeyedService<ProjectionTimelineService>(primaryContextName));
        }

        return new ProjectionDebugBuilder(services, rootOpts, endpoint);
    }

    /// <summary>
    /// Maps the projection time-travel debug HTTP endpoints under the given route prefix.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Registered endpoints:
    /// <list type="bullet">
    ///   <item><c>GET {prefix}/projections</c> — lists all registered projections.</item>
    ///   <item><c>GET {prefix}/projections/{name}/instances</c> — paged, tenant-scoped instance list.</item>
///   <item><c>GET {prefix}/projections/{name}/instances/{id}/timeline</c> — paged applied-event timeline.</item>
///   <item><c>GET {prefix}/projections/{name}/instances/{id}/state</c> — view state at a cutoff.</item>
///   <item><c>POST {prefix}/projections/{name}/instances/{id}/step</c> — step forward or back with diff.</item>
    /// </list>
    /// </para>
    /// <para>
    /// Call <c>AddProjectionDebugServices()</c> before building the app, and ensure
    /// <c>app.UseAuthentication()</c> and <c>app.UseAuthorization()</c> have been called
    /// before this method if <see cref="ProjectionDebugApiOptions.RequireAuthentication"/> is <see langword="true"/>.
    /// </para>
    /// </remarks>
    /// <param name="app">The endpoint route builder.</param>
    /// <param name="configure">Optional callback to configure <see cref="ProjectionDebugApiOptions"/>.</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    /// <example>
    /// <code>
    /// app.UseAuthentication();
    /// app.UseAuthorization();
    /// app.MapProjectionDebugApi(opts =>
    /// {
    ///     opts.RoutePrefix              = "/internal/projections/debug";
    ///     opts.RequireAuthentication    = true;
    ///     opts.AuthorizationPolicyNames = ["InternalAdmin"];
    /// });
    /// </code>
    /// </example>
    public static IEndpointRouteBuilder MapProjectionDebugApi(
        this IEndpointRouteBuilder app,
        Action<ProjectionDebugApiOptions>? configure = null)
    {
        var rootOpts = app.ServiceProvider.GetService<ProjectionDebugOptions>() ?? new ProjectionDebugOptions();

        if (rootOpts.Endpoints.Count == 0)
        {
            configure?.Invoke(rootOpts.Api);
            MapProjectionDebugApiForEndpoint(app, rootOpts.Api, endpoint: null, routeSuffix: "Default");
            return app;
        }

        var multiEndpoint = rootOpts.Endpoints.Count > 1;
        foreach (var endpoint in rootOpts.Endpoints)
        {
            ApplyProjectionDebugApiConfigure(configure, endpoint.Api, multiEndpoint);
            var routeSuffix = endpoint.DebugServiceKey ?? "Default";
            MapProjectionDebugApiForEndpoint(app, endpoint.Api, endpoint, routeSuffix);
        }

        return app;
    }

    // ── Debug API helpers ─────────────────────────────────────────────────────

    private static void ApplyProjectionDebugApiConfigure(
        Action<ProjectionDebugApiOptions>? configure,
        ProjectionDebugApiOptions endpointApi,
        bool multiEndpoint)
    {
        if (configure is null)
            return;

        if (!multiEndpoint)
        {
            configure(endpointApi);
            return;
        }

        var template = new ProjectionDebugApiOptions();
        configure(template);
        endpointApi.RequireAuthentication    = template.RequireAuthentication;
        endpointApi.AuthorizationPolicyNames = template.AuthorizationPolicyNames.ToArray();
        endpointApi.DefaultInstancePageSize  = template.DefaultInstancePageSize;
    }

    private static void MapProjectionDebugApiForEndpoint(
        IEndpointRouteBuilder app,
        ProjectionDebugApiOptions opts,
        ProjectionDebugEndpointRegistration? endpoint,
        string routeSuffix)
    {
        var prefix        = opts.RoutePrefix.TrimEnd('/');
        var policyNames   = opts.AuthorizationPolicyNames;
        var requireAuth   = opts.RequireAuthentication;
        var defaultPgSize = opts.DefaultInstancePageSize;

        // ── GET /projections ────────────────────────────────────────────────

        var listProjections = app.MapGet($"{prefix}/projections", (HttpContext ctx) =>
        {
            if (requireAuth && !(ctx.User.Identity?.IsAuthenticated ?? false))
                return Results.Unauthorized();

            var services = ResolveProjectionDebugEndpointServices(ctx.RequestServices, endpoint);
            var catalog = new ProjectionRegistrationCatalog(services.Registrations);

            var result = catalog.Families.Select(f => new
            {
                f.LogicalName,
                f.KebabName,
                LatestVersion = f.Latest.Version,
                LatestStorageKey = f.Latest.StorageKey,
                LatestResolutionMode = f.LatestResolutionMode.ToString(),
                Versions = f.Versions.Select(v => new
                {
                    v.Version,
                    v.StorageKey,
                    Kind = v.Kind.ToString(),
                    TenantScope = v.TenantScope.ToString(),
                    StreamType = v.StreamType ?? "(n/a)",
                    v.IsLatest,
                    v.DeclaredAsLatest,
                    v.DeprecationDateIso,
                    HasEndpoint = v.Endpoint is not null,
                    Endpoint = v.Endpoint is null ? null : BuildVersionedRoute(v.Endpoint.Route, v.Version)
                })
            });

            return Results.Ok(new { Count = catalog.Families.Count, Projections = result });
        })
        .WithName($"DebugListProjections_{routeSuffix}")
        .WithTags("ProjectionDebug")
        .WithSummary("List all registered lightweight projections.")
        .Produces(200)
        .Produces(401);

        ApplyAuth(listProjections, requireAuth, policyNames);

        // ── GET /projections/{name}/instances ───────────────────────────────

        var listInstances = app.MapGet(
            $"{prefix}/projections/{{name}}/instances",
            async (HttpContext ctx, string name, int? version = null, int page = 0, int? pageSize = null) =>
        {
            if (requireAuth && !(ctx.User.Identity?.IsAuthenticated ?? false))
                return Results.Unauthorized();

            var services = ResolveProjectionDebugEndpointServices(ctx.RequestServices, endpoint);
            var catalog = new ProjectionRegistrationCatalog(services.Registrations);
            ProjectionRegistration reg;

            try
            {
                reg = catalog.Resolve(name, version);
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(new { Error = ex.Message });
            }

            // Require tenant claim for TenantScoped projections.
            string? tenantId = null;
            if (reg.TenantScope == TenantScope.TenantScoped)
            {
                tenantId = ctx.User.FindFirst("tenant_id")?.Value
                    ?? ctx.User.FindFirst("tid")?.Value;
                if (string.IsNullOrEmpty(tenantId))
                    return Results.Unauthorized();
            }

            var result = await services.Timeline.GetInstancePageAsync(
                name, tenantId, page, pageSize ?? defaultPgSize, ctx.RequestAborted, version);

            return Results.Ok(result);
        })
        .WithName($"DebugListInstances_{routeSuffix}")
        .WithTags("ProjectionDebug")
        .WithSummary("List stored view instance IDs for a projection (paginated, tenant-scoped).")
        .Produces(200)
        .Produces(400)
        .Produces(401);

        ApplyAuth(listInstances, requireAuth, policyNames);

        // ── GET /projections/{name}/instances/{**id}/timeline ───────────────

        var getTimeline = app.MapGet(
            $"{prefix}/projections/{{name}}/instances/{{id}}/timeline",
            async (HttpContext ctx, string name, string id, int? version = null, int fromIndex = 0, int pageSize = 50) =>
        {
            if (requireAuth && !(ctx.User.Identity?.IsAuthenticated ?? false))
                return Results.Unauthorized();

            var instanceId = Uri.UnescapeDataString(id);

            try
            {
                var services = ResolveProjectionDebugEndpointServices(ctx.RequestServices, endpoint);
                var result = await services.Timeline.GetTimelinePageAsync(
                    name, instanceId, fromIndex, pageSize, ctx.RequestAborted, version);
                return Results.Ok(result);
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(new { Error = ex.Message });
            }
        })
        .WithName($"DebugGetTimeline_{routeSuffix}")
        .WithTags("ProjectionDebug")
        .WithSummary("Get a paged slice of the applied-event timeline for a projection instance.")
        .Produces(200)
        .Produces(400)
        .Produces(401);

        ApplyAuth(getTimeline, requireAuth, policyNames);

        // ── GET /projections/{name}/instances/{**id}/state ──────────────────

        var getState = app.MapGet(
            $"{prefix}/projections/{{name}}/instances/{{id}}/state",
            async (
                HttpContext ctx,
                string name,
                string id,
                int? version = null,
                long? atSequence   = null,
                long? atVersion    = null,
                DateTime? atTimestamp  = null,
                long? atAppliedIndex  = null) =>
        {
            if (requireAuth && !(ctx.User.Identity?.IsAuthenticated ?? false))
                return Results.Unauthorized();

            var setCutoffs = new[] { atSequence.HasValue, atVersion.HasValue,
                                     atTimestamp.HasValue, atAppliedIndex.HasValue };
            if (setCutoffs.Count(x => x) != 1)
                return Results.BadRequest(new
                {
                    Error = "Provide exactly one of: atSequence, atVersion, atTimestamp, or atAppliedIndex."
                });

            var instanceId = Uri.UnescapeDataString(id);

            try
            {
                var services = ResolveProjectionDebugEndpointServices(ctx.RequestServices, endpoint);

                if (atAppliedIndex.HasValue)
                {
                    // atAppliedIndex is 0-based last-event index; appliedCount = atAppliedIndex + 1.
                    var result = await services.Timeline.BuildAtAppliedIndexAsync(
                        name, instanceId, atAppliedIndex.Value + 1, ctx.RequestAborted, version);
                    return Results.Ok(ToDebugStateResponse(result));
                }
                else
                {
                    var cutoff = atSequence.HasValue ? ProjectionCutoff.AtSequence(atSequence.Value)
                               : atVersion.HasValue  ? ProjectionCutoff.AtVersion(atVersion.Value)
                               :                       ProjectionCutoff.AtTimestamp(atTimestamp!.Value);

                    var result = await services.AdHoc.BuildAtAsync(name, instanceId, cutoff, ctx.RequestAborted, version);
                    return Results.Ok(ToDebugStateResponse(result));
                }
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(new { Error = ex.Message });
            }
        })
        .WithName($"DebugGetState_{routeSuffix}")
        .WithTags("ProjectionDebug")
        .WithSummary("Get the projection view state at a specific cutoff point.")
        .Produces(200)
        .Produces(400)
        .Produces(401);

        ApplyAuth(getState, requireAuth, policyNames);

        // ── POST /projections/{name}/instances/{**id}/step ──────────────────

        var postStep = app.MapPost(
            $"{prefix}/projections/{{name}}/instances/{{id}}/step",
            async (HttpContext ctx, string name, string id, int? version = null) =>
        {
            if (requireAuth && !(ctx.User.Identity?.IsAuthenticated ?? false))
                return Results.Unauthorized();

            ProjectionDebugStepRequest? body;
            try
            {
                body = await JsonSerializer.DeserializeAsync<ProjectionDebugStepRequest>(
                    ctx.Request.Body,
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true },
                    ctx.RequestAborted);
            }
            catch
            {
                return Results.BadRequest(new
                {
                    Error = "Invalid request body. Expected: { \"direction\": \"forward\"|\"back\", \"currentAppliedIndex\": N }"
                });
            }

            if (body is null)
                return Results.BadRequest(new { Error = "Request body is required." });

            if (!string.Equals(body.Direction, "forward", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(body.Direction, "back", StringComparison.OrdinalIgnoreCase))
                return Results.BadRequest(new { Error = "Direction must be \"forward\" or \"back\"." });

            bool isForward = string.Equals(body.Direction, "forward", StringComparison.OrdinalIgnoreCase);
            long targetIndex = isForward
                ? body.CurrentAppliedIndex + 1
                : body.CurrentAppliedIndex - 1;

            var instanceId = Uri.UnescapeDataString(id);

            try
            {
                var services = ResolveProjectionDebugEndpointServices(ctx.RequestServices, endpoint);
                var (atTarget, atPrevious, targetEvent) = await services.Timeline.BuildStepAsync(
                    name, instanceId, targetIndex, ctx.RequestAborted, version);

                ViewStateDiff? diff = null;
                if (atPrevious is not null)
                    diff = ProjectionTimelineService.ComputeDiff(atPrevious.ViewJson, atTarget.ViewJson);

                return Results.Ok(new
                {
                    AppliedIndex = targetIndex,
                    Event        = targetEvent,
                    State        = ToDebugStateResponse(atTarget),
                    Diff         = diff
                });
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(new { Error = ex.Message });
            }
        })
        .WithName($"DebugStep_{routeSuffix}")
        .WithTags("ProjectionDebug")
        .WithSummary("Step forward or backward one event through a projection instance, returning state and diff.")
        .Accepts<ProjectionDebugStepRequest>("application/json")
        .Produces(200)
        .Produces(400)
        .Produces(401);

        ApplyAuth(postStep, requireAuth, policyNames);
    }

    private static ProjectionDebugEndpointRegistration CreateEndpointRegistration(
        ProjectionDebugOptions rootOpts,
        string? debugServiceKey = null,
        string? primaryContextName = null,
        string[]? additionalContextNames = null)
    {
        var endpoint = new ProjectionDebugEndpointRegistration
        {
            DebugServiceKey = debugServiceKey,
            PrimaryContextName = primaryContextName,
            AdditionalContextNames = additionalContextNames ?? []
        };

        CopyProjectionDebugApiDefaults(rootOpts.Api, endpoint.Api);
        rootOpts.Endpoints.Add(endpoint);
        return endpoint;
    }

    private static void CopyProjectionDebugApiDefaults(
        ProjectionDebugApiOptions source,
        ProjectionDebugApiOptions target)
    {
        target.RoutePrefix           = source.RoutePrefix;
        target.RequireAuthentication = source.RequireAuthentication;
        target.AuthorizationPolicyNames = source.AuthorizationPolicyNames.ToArray();
        target.DefaultInstancePageSize = source.DefaultInstancePageSize;
    }

    private static int CountContextScopedEndpoints(ProjectionDebugOptions rootOpts)
        => rootOpts.Endpoints.Count(e => e.DebugServiceKey is not null);

    private static void RegisterKeyedProjectionDebugServices(
        IServiceCollection services,
        string primaryContextName,
        string[] additionalContextNames)
    {
        services.TryAddKeyedSingleton<ProjectionTimelineService>(primaryContextName, (sp, _) =>
            new ProjectionTimelineService(
                sp.GetRequiredKeyedService<IEventStore>(primaryContextName),
                sp.GetRequiredKeyedService<IViewStore>(primaryContextName),
                ResolveProjectionRegistrations(sp, primaryContextName, additionalContextNames)));
    }

    private static void BridgeNonKeyedProjectionDebugServices(
        IServiceCollection services,
        string primaryContextName)
    {
        services.TryAddSingleton<IEventStore>(sp =>
            sp.GetRequiredKeyedService<IEventStore>(primaryContextName));

        services.TryAddSingleton<IViewStore>(sp =>
            sp.GetRequiredKeyedService<IViewStore>(primaryContextName));

        services.TryAddSingleton<IReadOnlyList<ProjectionRegistration>>(sp =>
            ResolveProjectionRegistrations(sp, primaryContextName, []));
    }

    private static IReadOnlyList<ProjectionRegistration> ResolveProjectionRegistrations(
        IServiceProvider sp,
        string primaryContextName,
        string[] additionalContextNames)
    {
        var all = new List<ProjectionRegistration>(
            sp.GetRequiredKeyedService<IReadOnlyList<ProjectionRegistration>>(primaryContextName));

        foreach (var ctx in additionalContextNames)
        {
            if (string.IsNullOrWhiteSpace(ctx)) continue;
            var extra = sp.GetKeyedService<IReadOnlyList<ProjectionRegistration>>(ctx);
            if (extra is not null)
                all.AddRange(extra);
        }

        return all;
    }

    private static ProjectionDebugEndpointServices ResolveProjectionDebugEndpointServices(
        IServiceProvider sp,
        ProjectionDebugEndpointRegistration? endpoint)
    {
        if (endpoint?.DebugServiceKey is null)
        {
            var timeline = sp.GetRequiredService<ProjectionTimelineService>();
            var registrations = sp.GetService<IReadOnlyList<ProjectionRegistration>>() ?? [];
            var adHoc = sp.GetService<AdHocProjectionBuilder>()
                ?? new AdHocProjectionBuilder(
                    sp.GetRequiredService<IEventStore>(),
                    registrations);
            return new ProjectionDebugEndpointServices(timeline, registrations, adHoc);
        }

        var contextKey = endpoint.DebugServiceKey;
        var contextRegistrations = ResolveProjectionRegistrations(
            sp,
            endpoint.PrimaryContextName ?? contextKey,
            endpoint.AdditionalContextNames);

        return new ProjectionDebugEndpointServices(
            sp.GetRequiredKeyedService<ProjectionTimelineService>(contextKey),
            contextRegistrations,
            new AdHocProjectionBuilder(
                sp.GetRequiredKeyedService<IEventStore>(contextKey),
                contextRegistrations));
    }

    private static void ApplyAuth(
        RouteHandlerBuilder builder,
        bool requireAuth,
        string[] policyNames)
        => ProjectionDebugEndpointAuth.Apply(builder, requireAuth, policyNames);

    private static ProjectionDebugOptions EnsureDebugOptions(IServiceCollection services)
    {
        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType == typeof(ProjectionDebugOptions)
                && descriptor.ImplementationInstance is ProjectionDebugOptions existing)
            {
                return existing;
            }
        }

        var created = new ProjectionDebugOptions();
        services.AddSingleton(created);
        return created;
    }

    private static object ToDebugStateResponse(TimeTravelResult result)
    {
        object? viewState = null;
        try { viewState = JsonSerializer.Deserialize<object>(result.ViewJson); }
        catch { viewState = result.ViewJson; }

        return new
        {
            result.LogicalName,
            result.ProjectionVersion,
            result.ProjectionStorageKey,
            result.IsLatest,
            result.LatestResolutionMode,
            ProjectionName = result.ProjectionName,
            result.InstanceId,
            result.ProjectionKind,
            result.RequestedCutoff,
            result.EventsApplied,
            result.FinalSequencePosition,
            result.FinalStreamVersion,
            FinalEventTimestamp = result.FinalEventTimestamp?.ToString("O"),
            ViewState = viewState
        };
    }

    // ── OpenTelemetry wiring helpers ──────────────────────────────────────────

    /// <summary>
    /// Adds the LawnDart lightweight projection <see cref="System.Diagnostics.Metrics.Meter"/>
    /// to an OpenTelemetry <see cref="MeterProviderBuilder"/> so that projection metrics are
    /// captured and exported by the configured OTel pipeline.
    /// </summary>
    /// <remarks>
    /// When using <c>AddServiceDefaults()</c> from <c>LawnDart.ServiceDefaults</c>
    /// this meter is already registered. Call this overload only if you configure OTel manually.
    /// </remarks>
    /// <example>
    /// <code>
    /// builder.Services.AddOpenTelemetry()
    ///     .WithMetrics(m => m.AddLightweightProjectionMetrics())
    ///     .WithTracing(t => t.AddLightweightProjectionTracing());
    /// </code>
    /// </example>
    public static MeterProviderBuilder AddLightweightProjectionMetrics(
        this MeterProviderBuilder builder)
        => builder.AddMeter(ProjectionTelemetry.MeterName);

    /// <summary>
    /// Adds the LawnDart lightweight projection <see cref="System.Diagnostics.ActivitySource"/>
    /// to an OpenTelemetry <see cref="TracerProviderBuilder"/> so that projection spans are
    /// captured and exported by the configured OTel pipeline.
    /// </summary>
    public static TracerProviderBuilder AddLightweightProjectionTracing(
        this TracerProviderBuilder builder)
        => builder.AddSource(ProjectionTelemetry.ActivitySourceName);
}
