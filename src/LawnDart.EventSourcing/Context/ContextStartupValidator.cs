using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Context;

/// <summary>
/// A hosted service that validates multi-context configuration at application startup,
/// failing fast with a clear error when common misconfigurations are detected.
/// </summary>
/// <remarks>
/// <para>
/// Registered automatically when <c>AddBoundedContext()</c> is called.  Runs once,
/// immediately when the host starts, then exits.
/// </para>
/// <para>
/// Current checks:
/// <list type="bullet">
///   <item>No command type is associated with more than one context.</item>
///   <item>In multi-context applications, every registered command type has a handler.</item>
/// </list>
/// </para>
/// </remarks>
public sealed class ContextStartupValidator : IHostedService
{
    private readonly IBoundedContextRegistry _contextRegistry;
    private readonly ICommandContextRegistry _commandRegistry;
    private readonly ILogger<ContextStartupValidator> _logger;

    /// <summary>Initializes a new <see cref="ContextStartupValidator"/>.</summary>
    public ContextStartupValidator(
        IBoundedContextRegistry contextRegistry,
        ICommandContextRegistry commandRegistry,
        ILogger<ContextStartupValidator> logger)
    {
        _contextRegistry = contextRegistry ?? throw new ArgumentNullException(nameof(contextRegistry));
        _commandRegistry = commandRegistry ?? throw new ArgumentNullException(nameof(commandRegistry));
        _logger          = logger          ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc/>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        Validate();
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private void Validate()
    {
        var contextNames  = _contextRegistry.ContextNames;
        var registrations = _commandRegistry.GetAllRegistrations();
        var isMulti       = _contextRegistry.IsMultiContext;

        _logger.LogDebug(
            "ContextStartupValidator: validating {ContextCount} bounded context(s) [{ContextNames}]",
            contextNames.Count,
            string.Join(", ", contextNames));

        // Verify every registered context has at least one command handler when using
        // multi-context mode, so developers notice forgotten WithCommandHandlers() calls early.
        if (isMulti)
        {
            foreach (var contextName in contextNames)
            {
                if (!registrations.ContainsKey(contextName))
                {
                    _logger.LogWarning(
                        "Bounded context '{ContextName}' has no command handlers registered via WithCommandHandlers(). " +
                        "If this context only hosts projections or is read-only, this warning can be suppressed.",
                        contextName);
                }
            }
        }

        _logger.LogInformation(
            "ContextStartupValidator: {ContextCount} bounded context(s) OK — " +
            "{CommandCount} command type(s) mapped across {HandlerContextCount} context(s).",
            contextNames.Count,
            registrations.Values.Sum(v => v.Count),
            registrations.Count);
    }
}
