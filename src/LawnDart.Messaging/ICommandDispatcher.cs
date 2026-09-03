namespace LawnDart.Messaging;

/// <summary>
/// Routes commands returned by reactors and task processors to the appropriate handler.
/// Register an implementation that resolves the target aggregate type from the command
/// and delegates to <c>IAggregateRepository.HandleCommandAsync</c> or similar.
/// </summary>
public interface ICommandDispatcher
{
    /// <summary>
    /// Dispatches a command to its handler.
    /// </summary>
    /// <param name="command">The command to dispatch.</param>
    /// <param name="context">Originating message context for correlation propagation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task DispatchAsync(ICommand command, MessageContext context, CancellationToken cancellationToken = default);
}
