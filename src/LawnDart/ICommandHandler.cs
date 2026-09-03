namespace LawnDart;

/// <summary>
/// Handles a specific command type by executing the associated business logic.
/// Implement this interface to define how a command is processed.
/// </summary>
/// <typeparam name="TCommand">The command type this handler processes.</typeparam>
public interface ICommandHandler<in TCommand> where TCommand : ICommand
{
    /// <summary>
    /// Executes the business logic for the specified command.
    /// </summary>
    /// <param name="command">The command to handle.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task HandleAsync(TCommand command, CancellationToken cancellationToken = default);
}
