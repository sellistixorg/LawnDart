using System.Diagnostics.CodeAnalysis;

namespace LawnDart.Patterns.Delegation;

/// <summary>
/// Delegates a command to another command (Command → Command pattern).
/// </summary>
/// <remarks>
/// Planned cell (🔧). No host in this version. Implementing this
/// interface does not register it with DI or cause the runtime to
/// invoke it.
/// </remarks>
/// <typeparam name="TCommand">The command type to delegate.</typeparam>
[Experimental("LAWNDART002")]
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
