using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using LawnDart.EventStore;
using LawnDart.Projections.Lightweight.Admin;
using LawnDart.Projections.Lightweight.Debug;
using LawnDart.Projections.Lightweight.Registration;

namespace LawnDart.Projections.Lightweight;

/// <summary>
/// Hosting extensions for the projection admin HTTP API (rebuild).
/// </summary>
public static class ProjectionAdminApiExtensions
{
    private const string RebuildDetail =
        "Views and checkpoints deleted; runner restarted cold and is replaying from sequence 0.";

    /// <summary>
    /// Maps <c>POST /{context}/projections/{name}/rebuild</c> for every bounded context
    /// that has a keyed <see cref="IProjectionAdmin"/> (registered by <c>WithProjections</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Discovery uses <see cref="IBoundedContextRegistry"/> plus keyed
    /// <see cref="IProjectionAdmin"/> resolution. Contexts without an admin
    /// (e.g. unkeyed <c>AddLightweightProjections</c> only) are skipped.
    /// </para>
    /// <para>
    /// Call after <c>UseAuthentication</c> / <c>UseAuthorization</c> when
    /// <see cref="ProjectionAdminApiOptions.RequireAuthentication"/> is
    /// <see langword="true"/> (the default).
    /// </para>
    /// <para>
    /// Status codes: <c>202</c> rebuild orchestration started; <c>404</c> unknown
    /// logical name/version; <c>409</c> multi-node interlock; <c>401</c>/<c>403</c>
    /// when auth is configured and fails.
    /// </para>
    /// </remarks>
    /// <param name="app">The endpoint route builder.</param>
    /// <param name="configure">Optional callback to configure admin API options.</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    /// <example>
    /// <code>
    /// app.UseAuthentication();
    /// app.UseAuthorization();
    /// app.MapProjectionAdminApi(opts =>
    /// {
    ///     opts.RequireAuthentication = true;
    ///     opts.AuthorizationPolicyNames = ["RequireSystemPermission:System.Impersonate"];
    /// });
    /// </code>
    /// </example>
    public static IEndpointRouteBuilder MapProjectionAdminApi(
        this IEndpointRouteBuilder app,
        Action<ProjectionAdminApiOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(app);

        var registration = app.ServiceProvider.GetService<ProjectionAdminApiRegistration>();
        var options = registration?.Options ?? new ProjectionAdminApiOptions();
        configure?.Invoke(options);

        if (registration is not null)
            registration.Options = options;

        if (!options.EnableRebuildEndpoints)
            return app;

        var registry = app.ServiceProvider.GetService<IBoundedContextRegistry>();
        if (registry is null || registry.ContextNames.Count == 0)
            return app;

        foreach (var contextName in registry.ContextNames)
        {
            if (string.IsNullOrWhiteSpace(contextName))
                continue;

            if (app.ServiceProvider.GetKeyedService<IProjectionAdmin>(contextName) is null)
                continue;

            MapRebuildEndpoint(app, contextName, options);
            registration?.MarkMapped(contextName);
        }

        return app;
    }

    private static void MapRebuildEndpoint(
        IEndpointRouteBuilder app,
        string contextName,
        ProjectionAdminApiOptions options)
    {
        var requireAuth = options.RequireAuthentication;
        var policyNames = options.AuthorizationPolicyNames;
        var capturedContext = contextName;

        var rebuild = app.MapPost(
            $"/{capturedContext}/projections/{{name}}/rebuild",
            async (HttpContext ctx, string name, int? version = null, CancellationToken ct = default) =>
            {
                if (requireAuth && !(ctx.User.Identity?.IsAuthenticated ?? false))
                    return Results.Unauthorized();

                var catalog = ctx.RequestServices
                    .GetRequiredKeyedService<ProjectionRegistrationCatalog>(capturedContext);

                ProjectionRegistration reg;
                try
                {
                    reg = catalog.Resolve(name, version);
                }
                catch (InvalidOperationException ex)
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status404NotFound,
                        title: "Unknown projection",
                        detail: ex.Message);
                }

                var admin = ctx.RequestServices
                    .GetRequiredKeyedService<IProjectionAdmin>(capturedContext);

                try
                {
                    await admin.RebuildAsync(name, version, ct).ConfigureAwait(false);
                }
                catch (InvalidOperationException ex)
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status404NotFound,
                        title: "Unknown projection",
                        detail: ex.Message);
                }
                catch (NotSupportedException ex)
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status409Conflict,
                        title: "Rebuild not supported in this deployment",
                        detail: ex.Message);
                }

                return Results.Accepted(
                    uri: (string?)null,
                    value: new
                    {
                        context = capturedContext,
                        projection = name,
                        version = reg.Version,
                        storageKey = reg.StorageKey,
                        status = "rebuilding",
                        detail = RebuildDetail
                    });
            })
            .WithName($"RebuildProjection_{capturedContext}")
            .WithTags("ProjectionAdmin")
            .WithSummary($"Rebuild a lightweight projection in the '{capturedContext}' bounded context.")
            .Produces(StatusCodes.Status202Accepted)
            .Produces(StatusCodes.Status401Unauthorized)
            .ProducesProblem(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status409Conflict);

        ProjectionDebugEndpointAuth.Apply(rebuild, requireAuth, policyNames);
    }
}
