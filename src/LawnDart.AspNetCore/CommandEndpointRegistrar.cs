using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using LawnDart.Authorization;
using LawnDart.EventStore;
using LawnDart.Messaging;

namespace LawnDart.AspNetCore;

/// <summary>
/// Discovers <see cref="ICommandHandler{TCommand}"/> implementations in the registered assemblies
/// and maps each one to a convention-derived HTTP POST endpoint.
/// </summary>
internal static class CommandEndpointRegistrar
{
    private static readonly Type CommandHandlerOpenType = typeof(ICommandHandler<>);
    private static readonly ActivitySource ActivitySource = new("LawnDart.AspNetCore");

    internal const string DefaultContextName = "default";

    /// <summary>
    /// <c>true</c> when HTTP should resolve unkeyed handlers (no context name, or
    /// the conventional <c>"default"</c> context with unkeyed store aliases).
    /// </summary>
    internal static bool IsUnkeyedContext(string? contextName) =>
        string.IsNullOrEmpty(contextName)
        || string.Equals(contextName, DefaultContextName, StringComparison.Ordinal);

    /// <summary>
    /// Finds all concrete <see cref="ICommandHandler{TCommand}"/> types across the given assemblies,
    /// derives their routes, and registers them as Minimal API endpoints.
    /// </summary>
    internal static void RegisterAll(
        IEndpointRouteBuilder app,
        IEnumerable<Assembly> assemblies,
        HttpCommandOptions options,
        string? contextName = null)
    {
        foreach (var assembly in assemblies)
        {
            foreach (var handlerType in DiscoverHandlers(assembly))
            {
                var commandType = GetCommandType(handlerType);
                if (commandType is null)
                    continue;

                RegisterEndpoint(app, handlerType, commandType, options, contextName);
            }
        }
    }

    /// <summary>
    /// Finds all non-abstract, non-interface types in the assembly that implement
    /// <see cref="ICommandHandler{TCommand}"/>.
    /// </summary>
    internal static IEnumerable<Type> DiscoverHandlers(Assembly assembly)
    {
        return assembly.GetTypes()
            .Where(t => t is { IsAbstract: false, IsInterface: false }
                        && t.GetInterfaces().Any(IsCommandHandlerInterface));
    }

    /// <summary>
    /// Extracts the TCommand type argument from a handler type's ICommandHandler&lt;TCommand&gt; interface.
    /// </summary>
    internal static Type? GetCommandType(Type handlerType)
    {
        return handlerType
            .GetInterfaces()
            .FirstOrDefault(IsCommandHandlerInterface)
            ?.GetGenericArguments()[0];
    }

    /// <summary>
    /// Derives the kebab-case route group from a command type's namespace.
    /// E.g. <c>Acme.Orders.Commands.CreateOrderCommand</c> → <c>orders</c>.
    /// </summary>
    internal static string DeriveGroup(Type commandType, HttpCommandOptions options)
    {
        if (options.TagStrategy == CommandTagStrategy.Flat)
            return options.FallbackTag.ToLowerInvariant();

        var ns = commandType.Namespace ?? string.Empty;
        var segments = ns.Split('.', StringSplitOptions.RemoveEmptyEntries);

        // Skip well-known framework segments and the "Commands" segment itself
        var meaningful = segments
            .Where(s => !s.Equals("Commands", StringComparison.OrdinalIgnoreCase)
                        && !s.Equals("Patterns", StringComparison.OrdinalIgnoreCase))
            .ToList();

        // Use the last meaningful segment as the group
        var group = meaningful.Count > 0
            ? meaningful[^1]
            : options.FallbackTag;

        return ToKebabCase(group);
    }

    /// <summary>
    /// Derives the kebab-case action name from a command type name.
    /// E.g. <c>CreateOrderCommand</c> → <c>create-order</c>.
    /// </summary>
    internal static string DeriveActionName(Type commandType)
    {
        var name = commandType.Name;

        // Strip "Command" suffix
        if (name.EndsWith("Command", StringComparison.OrdinalIgnoreCase))
            name = name[..^"Command".Length];

        return ToKebabCase(name);
    }

    /// <summary>
    /// Derives the full route pattern for a command type.
    /// E.g. <c>CreateOrderCommand</c> in namespace <c>Acme.Orders.Commands</c>
    /// with prefix <c>api</c> → <c>/api/orders/create-order</c>.
    /// </summary>
    internal static string DeriveRoute(Type commandType, HttpCommandOptions options)
    {
        var prefix = options.RoutePrefix.Trim('/');
        var group = DeriveGroup(commandType, options);
        var action = DeriveActionName(commandType);

        return string.IsNullOrEmpty(prefix)
            ? $"/{group}/{action}"
            : $"/{prefix}/{group}/{action}";
    }

    /// <summary>
    /// Converts a PascalCase identifier to kebab-case.
    /// E.g. <c>CreateOrder</c> → <c>create-order</c>.
    /// </summary>
    internal static string ToKebabCase(string value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        var result = new StringBuilder();
        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            if (char.IsUpper(c) && i > 0)
                result.Append('-');
            result.Append(char.ToLowerInvariant(c));
        }

        return result.ToString();
    }

    /// <summary>
    /// Converts a PascalCase or kebab-case identifier into a human-readable summary.
    /// E.g. <c>CreateOrderCommand</c> → <c>Create Order</c>.
    /// </summary>
    internal static string HumanizeName(Type commandType)
    {
        var name = commandType.Name;
        if (name.EndsWith("Command", StringComparison.OrdinalIgnoreCase))
            name = name[..^"Command".Length];

        return Regex.Replace(name, "([A-Z])", " $1").Trim();
    }

    private static void RegisterEndpoint(
        IEndpointRouteBuilder app,
        Type handlerType,
        Type commandType,
        HttpCommandOptions options,
        string? contextName)
    {
        var route = DeriveRoute(commandType, options);
        var tag = DeriveGroup(commandType, options);
        var summary = HumanizeName(commandType);

        // Build the endpoint delegate using reflection so we can work with the generic types
        var method = typeof(CommandEndpointRegistrar)
            .GetMethod(nameof(BuildEndpointDelegate), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(commandType);

        var delegateObj = method.Invoke(null, [contextName])!;
        var requestDelegate = (Delegate)delegateObj;

        app.MapPost(route, requestDelegate)
            .WithTags(char.ToUpperInvariant(tag[0]) + tag[1..])
            .WithSummary(summary)
            .WithDescription($"Executes the {summary} command.")
            .Produces(StatusCodes.Status202Accepted)
            .Produces<ProblemDetails>(StatusCodes.Status403Forbidden, "application/problem+json")
            .Produces<ProblemDetails>(StatusCodes.Status409Conflict, "application/problem+json")
            .Produces<ProblemDetails>(StatusCodes.Status422UnprocessableEntity, "application/problem+json");
    }

    private static Delegate BuildEndpointDelegate<TCommand>(string? contextName) where TCommand : ICommand
    {
        // Compute once at registration time — avoids per-request reflection
        var hasAuthRequirements = typeof(TCommand).GetCustomAttributes(inherit: true)
            .Any(a => a is RequiresPermissionAttribute
                   || a is RequiresEntitlementAttribute
                   || a is RequiresPolicyAttribute);

        var keyed = !IsUnkeyedContext(contextName);
        var key = contextName;

        if (hasAuthRequirements)
        {
            if (keyed)
            {
                return async (
                    TCommand command,
                    HttpContext http,
                    AuthorizationService authService,
                    CancellationToken ct) =>
                {
                    var handler = http.RequestServices.GetRequiredKeyedService<ICommandHandler<TCommand>>(key!);
                    var authResult = await authService.AuthorizeCommandAsync(command, ct);
                    if (!authResult.IsAuthorized)
                        return Results.Problem(
                            title: "Forbidden",
                            detail: string.Join("; ", authResult.FailedChecks),
                            statusCode: StatusCodes.Status403Forbidden);

                    return await ExecuteHttpCommandAsync(command, http, handler, ct);
                };
            }

            return async (
                TCommand command,
                HttpContext http,
                ICommandHandler<TCommand> handler,
                AuthorizationService authService,
                CancellationToken ct) =>
            {
                var authResult = await authService.AuthorizeCommandAsync(command, ct);
                if (!authResult.IsAuthorized)
                    return Results.Problem(
                        title: "Forbidden",
                        detail: string.Join("; ", authResult.FailedChecks),
                        statusCode: StatusCodes.Status403Forbidden);

                return await ExecuteHttpCommandAsync(command, http, handler, ct);
            };
        }

        if (keyed)
        {
            return async (
                TCommand command,
                HttpContext http,
                CancellationToken ct) =>
            {
                var handler = http.RequestServices.GetRequiredKeyedService<ICommandHandler<TCommand>>(key!);
                return await ExecuteHttpCommandAsync(command, http, handler, ct);
            };
        }

        // No auth attributes — publicly accessible, skip auth entirely
        return async (
            TCommand command,
            HttpContext http,
            ICommandHandler<TCommand> handler,
            CancellationToken ct) =>
        {
            return await ExecuteHttpCommandAsync(command, http, handler, ct);
        };
    }

    private static async Task<IResult> ExecuteHttpCommandAsync<TCommand>(
        TCommand command,
        HttpContext http,
        ICommandHandler<TCommand> handler,
        CancellationToken ct)
        where TCommand : ICommand
    {
        AssignCommandIdIfEmpty(command, http);
        var message = CreateInboundContext(http);
        using var activity = MessageTrace.Start(
            ActivitySource,
            $"http.command.{typeof(TCommand).Name}",
            message,
            ActivityKind.Server);
        using var ambient = AmbientMessageContext.Push(message);
        return await ExecuteHandlerAsync(command, handler, ct);
    }

    private static void AssignCommandIdIfEmpty<TCommand>(TCommand command, HttpContext http)
        where TCommand : ICommand
    {
        if (command.Id != Guid.Empty)
            return;

        var raw = http.Request.Headers["Idempotency-Key"].FirstOrDefault();
        var id = Guid.TryParse(raw, out var parsed) ? parsed : Guid.NewGuid();
        var property = typeof(TCommand).GetProperty(nameof(ICommand.Id));
        if (property?.SetMethod is not null)
            property.SetValue(command, id);
    }

    private static MessageContext CreateInboundContext(HttpContext http)
    {
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (http.Request.Headers.TryGetValue("traceparent", out var traceparent) &&
            !string.IsNullOrWhiteSpace(traceparent))
        {
            headers[MessageTrace.TraceParentHeader] = traceparent.ToString();
        }

        if (http.Request.Headers.TryGetValue("tracestate", out var tracestate) &&
            !string.IsNullOrWhiteSpace(tracestate))
        {
            headers[MessageTrace.TraceStateHeader] = tracestate.ToString();
        }

        string? correlation = null;
        if (headers.TryGetValue(MessageTrace.TraceParentHeader, out var tp) &&
            ActivityContext.TryParse(tp, traceState: null, out var parent) &&
            parent != default)
        {
            correlation = parent.TraceId.ToHexString();
        }

        var idempotency = http.Request.Headers["Idempotency-Key"].FirstOrDefault();

        return new MessageContext
        {
            MessageId = string.IsNullOrWhiteSpace(idempotency) ? Guid.NewGuid().ToString() : idempotency,
            CorrelationId = correlation,
            TenantId = http.Request.Headers["X-Tenant-Id"].FirstOrDefault(),
            UserId = http.User.FindFirst("sub")?.Value,
            TransportType = "HTTP",
            Headers = headers
        };
    }

    private static async Task<IResult> ExecuteHandlerAsync<TCommand>(
        TCommand command,
        ICommandHandler<TCommand> handler,
        CancellationToken ct)
        where TCommand : ICommand
    {
        try
        {
            await handler.HandleAsync(command, ct);
            return Results.Accepted();
        }
        catch (DomainException ex)
        {
            return Results.Problem(
                title: ex.Title,
                detail: ex.Message,
                statusCode: StatusCodes.Status422UnprocessableEntity);
        }
        catch (ConcurrencyException ex)
        {
            return Results.Problem(
                title: "Conflict",
                detail: ex.Message,
                statusCode: StatusCodes.Status409Conflict);
        }
    }

    private static bool IsCommandHandlerInterface(Type t) =>
        t.IsGenericType && t.GetGenericTypeDefinition() == CommandHandlerOpenType;
}
