using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using LawnDart.Messaging;

namespace LawnDart.EventSourcing.Context;

/// <summary>
/// An <see cref="ICommandDispatcher"/> implementation that routes each command to the
/// keyed <c>ICommandHandler&lt;TCommand&gt;</c> registered for the command's bounded context.
/// </summary>
/// <remarks>
/// <para>
/// Register via <c>services.AddContextAwareCommandDispatcher()</c>.  Once registered, the
/// standard Minimal API / MVC endpoint code calls
/// <c>dispatcher.DispatchAsync(command, ctx)</c> without knowing the context name.
/// </para>
/// <para>
/// The handler is resolved as a <em>keyed</em> service using the context name returned by
/// <see cref="ICommandContextRegistry"/>.  Because handlers are created with a
/// <see cref="ContextServiceProvider"/>, their own injected dependencies
/// (e.g. <c>IAggregateRepository</c>) are automatically resolved from the correct context.
/// </para>
/// </remarks>
public sealed class ContextAwareCommandDispatcher : ICommandDispatcher
{
    private readonly ICommandContextRegistry _registry;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<ContextAwareCommandDispatcher>? _logger;

    /// <summary>Initializes a new instance of <see cref="ContextAwareCommandDispatcher"/>.</summary>
    public ContextAwareCommandDispatcher(
        ICommandContextRegistry registry,
        IServiceProvider serviceProvider,
        ILogger<ContextAwareCommandDispatcher>? logger = null)
    {
        _registry        = registry        ?? throw new ArgumentNullException(nameof(registry));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _logger          = logger;
    }

    /// <inheritdoc/>
    public Task DispatchAsync(
        ICommand command,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);

        var commandType = command.GetType();
        var contextName = _registry.GetContextName(commandType);

        _logger?.LogDebug(
            "Dispatching command {CommandType} to bounded context '{ContextName}'",
            commandType.Name, contextName);

        // Resolve the closed generic ICommandHandler<TCommand> for this context
        var handlerType = typeof(ICommandHandler<>).MakeGenericType(commandType);

        var handler = (_serviceProvider as IKeyedServiceProvider)
            ?.GetRequiredKeyedService(handlerType, contextName)
            ?? throw new InvalidOperationException(
                $"No keyed ICommandHandler<{commandType.Name}> registered for context '{contextName}'. " +
                "Ensure WithCommandHandlers() has been called on the BoundedContextBuilder for this context.");

        // Invoke HandleAsync(command, cancellationToken) through the closed interface
        var handleMethod = handlerType.GetMethod(nameof(ICommandHandler<ICommand>.HandleAsync))
            ?? throw new InvalidOperationException(
                $"HandleAsync method not found on {handlerType.FullName}.");

        return (Task)handleMethod.Invoke(handler, [command, cancellationToken])!;
    }
}
