namespace LawnDart.Patterns.Delegation;

/// <summary>
/// Delegates a command to another command (Command → Command pattern).
/// </summary>
/// <typeparam name="TCommand">The command type to delegate.</typeparam>
public interface ICommandDelegator<TCommand> where TCommand : ICommand
{
    /// <summary>
    /// Delegates the command to another command.
    /// </summary>
    /// <param name="command">The command to delegate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The delegated command.</returns>
    Task<ICommand> DelegateAsync(TCommand command, CancellationToken cancellationToken = default);
}


