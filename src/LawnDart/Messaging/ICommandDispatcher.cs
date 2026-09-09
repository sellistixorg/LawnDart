namespace LawnDart.Messaging;

/// <summary>
/// Routes commands returned by reactors, task processors, and HTTP/job hosts
/// to the appropriate <see cref="ICommandHandler{TCommand}"/>.
/// </summary>
/// <remarks>
/// Lives in Core (same package as <see cref="MessageContext"/>).
/// EventSourcing registers <c>ContextAwareCommandDispatcher</c> from
/// <c>UseInMemory</c> / <c>UseSqlServer</c> / <c>WithCommandHandlers</c>.
/// Messaging reactors call this interface; they do not own it.
/// </remarks>
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
