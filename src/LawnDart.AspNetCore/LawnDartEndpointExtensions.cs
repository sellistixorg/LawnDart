using System.Reflection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using LawnDart.Authorization;

namespace LawnDart.AspNetCore;

/// <summary>
/// Extension methods for registering and mapping auto-discovered HTTP command endpoints.
/// </summary>
public static class LawnDartEndpointExtensions
{
    /// <summary>
    /// Records assemblies for HTTP command mapping on the conventional
    /// <c>"default"</c> context and wires authorization infrastructure.
    /// Does not register <see cref="ICommandHandler{TCommand}"/> — call
    /// <c>WithCommandHandlers&lt;TMarker&gt;()</c> on the bounded context.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="assemblies">
    /// One or more assemblies to scan for endpoints. Pass the assembly that contains
    /// your commands and handlers, e.g. <c>typeof(CreateOrderCommand).Assembly</c>.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// Use this overload for the conventional <c>"default"</c> context. Pair with
    /// <c>UseInMemory()</c> / <c>UseSqlServer()</c>, which register unkeyed store/repo
    /// aliases for <c>"default"</c>, and <c>WithCommandHandlers&lt;TMarker&gt;()</c>,
    /// which is the only handler registrar. For a named bounded context, use
    /// <see cref="AddLawnDartHttpCommands(IServiceCollection, string, Assembly[])"/>.
    /// </remarks>
    /// <example>
    /// <code>
    /// ctx.WithCommandHandlers&lt;CreateOrderCommand&gt;();
    /// builder.Services.AddLawnDartHttpCommands(typeof(CreateOrderCommand).Assembly);
    /// </code>
    /// </example>
    public static IServiceCollection AddLawnDartHttpCommands(
        this IServiceCollection services,
        params Assembly[] assemblies)
        => AddLawnDartHttpCommandsCore(services, contextName: null, assemblies);

    /// <summary>
    /// Records assemblies for HTTP command mapping on
    /// <paramref name="contextName"/>. Does not register
    /// <see cref="ICommandHandler{TCommand}"/> — call
    /// <c>WithCommandHandlers&lt;TMarker&gt;()</c> on that bounded context.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="contextName">
    /// Bounded-context DI key. <c>"default"</c> (or null) maps unkeyed handlers
    /// via the context's unkeyed aliases. Any other name maps keyed handlers.
    /// </param>
    /// <param name="assemblies">Assemblies that contain the commands and handlers.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddLawnDartHttpCommands(
        this IServiceCollection services,
        string contextName,
        params Assembly[] assemblies)
        => AddLawnDartHttpCommandsCore(services, contextName, assemblies);

    private static IServiceCollection AddLawnDartHttpCommandsCore(
        IServiceCollection services,
        string? contextName,
        Assembly[] assemblies)
    {
        // Routing + authorization infrastructure. Handlers are registered only by
        // WithCommandHandlers on the bounded context.
        services.AddRouting();
        services.AddHttpContextAccessor();
        if (!services.Any(d => d.ServiceType == typeof(IAuthorizationContextProvider)))
            services.AddSingleton<IAuthorizationContextProvider, MissingAuthorizationContextProvider>();
        services.AddScoped<AuthorizationService>();

        if (!services.Any(d => d.ServiceType == typeof(IAuthorizationProvider)))
            services.AddSingleton<IAuthorizationProvider, DefaultAuthorizationProvider>();

        GetOrCreateAssemblyRegistry(services).Add(contextName, assemblies);

        return services;
    }

    /// <summary>
    /// Maps all auto-discovered <see cref="ICommandHandler{TCommand}"/> implementations to
    /// convention-derived HTTP POST endpoints, complete with OpenAPI metadata and
    /// authorization checks drawn from the command's declarative attributes.
    /// </summary>
    /// <param name="app">The endpoint route builder.</param>
    /// <param name="configure">Optional callback to override default <see cref="HttpCommandOptions"/>.</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    /// <remarks>
    /// Resolves unkeyed handlers (the <c>"default"</c> single-context path).
    /// For a named bounded context, use
    /// <see cref="MapLawnDartCommands(IEndpointRouteBuilder, string, Action{HttpCommandOptions}?)"/>.
    /// </remarks>
    /// <example>
    /// <code>
    /// app.MapLawnDartCommands();
    ///
    /// // With custom prefix:
    /// app.MapLawnDartCommands(o => o.RoutePrefix = "v1");
    /// </code>
    /// </example>
    public static IEndpointRouteBuilder MapLawnDartCommands(
        this IEndpointRouteBuilder app,
        Action<HttpCommandOptions>? configure = null)
        => MapLawnDartCommandsCore(app, contextName: null, configure);

    /// <summary>
    /// Maps command endpoints for <paramref name="contextName"/>.
    /// </summary>
    /// <param name="app">The endpoint route builder.</param>
    /// <param name="contextName">
    /// Bounded-context DI key. <c>"default"</c> resolves unkeyed handlers; any other name
    /// resolves <c>GetRequiredKeyedService&lt;ICommandHandler&lt;T&gt;&gt;(contextName)</c>.
    /// </param>
    /// <param name="configure">Optional callback to override default <see cref="HttpCommandOptions"/>.</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    public static IEndpointRouteBuilder MapLawnDartCommands(
        this IEndpointRouteBuilder app,
        string contextName,
        Action<HttpCommandOptions>? configure = null)
        => MapLawnDartCommandsCore(app, contextName, configure);

    private static IEndpointRouteBuilder MapLawnDartCommandsCore(
        IEndpointRouteBuilder app,
        string? contextName,
        Action<HttpCommandOptions>? configure)
    {
        var options = new HttpCommandOptions();
        configure?.Invoke(options);

        var registry = app.ServiceProvider.GetRequiredService<CommandAssemblyRegistry>();
        CommandEndpointRegistrar.RegisterAll(app, registry.GetAssemblies(contextName), options, contextName);

        return app;
    }

    private static CommandAssemblyRegistry GetOrCreateAssemblyRegistry(IServiceCollection services)
    {
        for (var i = 0; i < services.Count; i++)
        {
            if (services[i].ServiceType == typeof(CommandAssemblyRegistry) &&
                services[i].ImplementationInstance is CommandAssemblyRegistry existing)
            {
                return existing;
            }
        }

        var registry = new CommandAssemblyRegistry();
        services.AddSingleton(registry);
        return registry;
    }
}

/// <summary>
/// Holds assemblies registered via <see cref="LawnDartEndpointExtensions"/>.
/// </summary>
internal sealed class CommandAssemblyRegistry
{
    private readonly List<CommandAssemblyEntry> _entries = [];

    internal void Add(string? contextName, IReadOnlyList<Assembly> assemblies)
        => _entries.Add(new CommandAssemblyEntry(contextName, assemblies.ToList()));

    internal IReadOnlyList<Assembly> GetAssemblies(string? contextName)
    {
        var matches = CommandEndpointRegistrar.IsUnkeyedContext(contextName)
            ? _entries.Where(e => CommandEndpointRegistrar.IsUnkeyedContext(e.ContextName))
            : _entries.Where(e => string.Equals(e.ContextName, contextName, StringComparison.Ordinal));

        var assemblies = matches.SelectMany(e => e.Assemblies).Distinct().ToList();
        if (assemblies.Count == 0)
        {
            var label = contextName ?? "default";
            throw new InvalidOperationException(
                $"No HTTP command assemblies registered for context '{label}'. " +
                $"Call AddLawnDartHttpCommands(\"{label}\", assembly) before MapLawnDartCommands.");
        }

        return assemblies;
    }

    private sealed record CommandAssemblyEntry(string? ContextName, IReadOnlyList<Assembly> Assemblies);
}

/// <summary>
/// No-op context provider so <see cref="AuthorizationService"/> can be constructed
/// without <c>LawnDart.Authorization.AspNetCore</c>. Commands with auth attributes
/// still fail until <c>AddHttpAuthorizationContext</c> is called.
/// </summary>
internal sealed class MissingAuthorizationContextProvider : IAuthorizationContextProvider
{
    public bool CanProvideContext() => false;

    public Task<AuthorizationContext?> GetAuthorizationContextAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<AuthorizationContext?>(null);
}
