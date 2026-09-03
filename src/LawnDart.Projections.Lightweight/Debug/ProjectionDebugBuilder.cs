using Microsoft.Extensions.DependencyInjection;

namespace LawnDart.Projections.Lightweight.Debug;

/// <summary>
/// Fluent builder returned by <c>AddProjectionDebugServices()</c>.
/// </summary>
public sealed class ProjectionDebugBuilder
{
    internal ProjectionDebugBuilder(
        IServiceCollection services,
        ProjectionDebugOptions rootOptions,
        ProjectionDebugEndpointRegistration endpoint)
    {
        Services = services;
        RootOptions = rootOptions;
        Endpoint = endpoint;
    }

    /// <summary>
    /// Gets the service collection being configured.
    /// </summary>
    public IServiceCollection Services { get; }

    internal ProjectionDebugOptions RootOptions { get; }

    internal ProjectionDebugEndpointRegistration Endpoint { get; }

    /// <summary>
    /// Enables the built-in static projection debugger UX at the given route.
    /// </summary>
    /// <param name="route">Route prefix for the debugger page (default <c>/projectiondebugger</c>).</param>
    /// <param name="configure">Optional callback to configure UX-specific options.</param>
    /// <returns>The builder for chaining.</returns>
    /// <remarks>
    /// Call <c>MapProjectionDebugUx()</c> from
    /// <c>LawnDart.Projections.Lightweight.Debug.Ui</c> after building the app.
    /// </remarks>
    public ProjectionDebugBuilder WithDebuggingUX(
        string route = "/projectiondebugger",
        Action<ProjectionDebugUxOptions>? configure = null)
    {
        Endpoint.Ux ??= new ProjectionDebugUxOptions();
        Endpoint.Ux.Enabled = true;
        Endpoint.Ux.Route = route.TrimEnd('/');

        if (Endpoint.Api.RoutePrefix == ProjectionDebugApiOptions.DefaultRoutePrefix)
            Endpoint.Api.RoutePrefix = $"{Endpoint.Ux.Route}/api";

        if (Endpoint.Ux.AuthorizationPolicyNames.Length == 0
            && Endpoint.Api.AuthorizationPolicyNames.Length > 0)
        {
            Endpoint.Ux.AuthorizationPolicyNames = Endpoint.Api.AuthorizationPolicyNames.ToArray();
        }

        Endpoint.Ux.RequireAuthentication = Endpoint.Api.RequireAuthentication;

        configure?.Invoke(Endpoint.Ux);
        return this;
    }
}
